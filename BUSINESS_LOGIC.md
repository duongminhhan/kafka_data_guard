# CDC abandoned-transaction exact replay

Đây là tài liệu SSOT cho business logic khôi phục event Debezium Oracle bị mất do
transaction bị abandoned. Cấu hình Grafana/alert nằm ngoài phạm vi tài liệu này.

## Nguyên tắc bất biến

App là pipeline **Oracle read-only → Kafka publish-only**:

- chỉ đọc metadata, redo/archive/LogMiner, Flashback và trạng thái hiện tại của source;
- chỉ tạo kết quả bằng cách publish repair message `c` hoặc `d` lên đúng Kafka topic;
- không thực thi DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) hoặc DDL lên database nguồn;
- không kết nối và không ghi vào database đích;
- không có Oracle side table lưu trạng thái hay chống trùng.

## 1. Tiếp nhận request

Webhook parse đầy đủ dòng log và publish request vào `cdc-remediation-requests`:

```json
{
  "connector": "oracle-remediation-poc",
  "transaction_id": "01001000E6030000",
  "detected_at": "2026-07-21 10:39:46+07:00",
  "log_line": "Transaction 01001000e6030000 (start SCN 6187821, change time 2026-07-21T03:39:46Z, redo thread 1, 5 events) is being abandoned."
}
```

Log có `0 events` được trả về `ignored`: không ghi request Kafka và không truy vấn
Oracle. Request Kafka chỉ commit offset sau khi toàn bộ XID đã được mining, kiểm tra và
publish thành công.

## 2. Xác định transaction và phạm vi connector

Consumer đọc cấu hình runtime của connector, sau đó dùng
`FLASHBACK_TRANSACTION_QUERY` để lấy bảng, số DML và khoảng SCN:

```sql
SELECT
    TABLE_OWNER,
    TABLE_NAME,
    OPERATION,
    COUNT(*) AS CHANGE_COUNT,
    MIN(START_SCN) AS START_SCN,
    MAX(COMMIT_SCN) AS COMMIT_SCN,
    MAX(COMMIT_TIMESTAMP) AS COMMIT_TIME
FROM FLASHBACK_TRANSACTION_QUERY
WHERE XID = HEXTORAW(:transaction_id)
  AND TABLE_OWNER IS NOT NULL
  AND TABLE_NAME IS NOT NULL
  AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
GROUP BY TABLE_OWNER, TABLE_NAME, OPERATION
ORDER BY TABLE_OWNER, TABLE_NAME, OPERATION;
```

Chỉ các bảng khớp `table.include.list` và không khớp `table.exclude.list` được replay.
Transaction chưa có `COMMIT_SCN` được retry. Query này chỉ dùng làm metadata và số lượng
đối chiếu; không dùng `UNDO_SQL` để tạo event.

## 3. Tự phát hiện redo/archive log

Service không cấu hình hoặc hard-code đường dẫn redo. Trong đúng Oracle connection sẽ
chạy LogMiner, service truy vấn `V$ARCHIVED_LOG`, `V$LOG` và `V$LOGFILE` để lấy các file
giao với `[START_SCN, COMMIT_SCN]`:

```sql
WITH CANDIDATES AS (
    SELECT THREAD#, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#, NAME,
           1 AS SOURCE_PRIORITY
    FROM V$ARCHIVED_LOG
    WHERE NAME IS NOT NULL
      AND DELETED = 'NO'
      AND STATUS = 'A'
      AND FIRST_CHANGE# <= :end_scn
      AND NEXT_CHANGE# > :start_scn

    UNION ALL

    SELECT L.THREAD#, L.SEQUENCE#, L.FIRST_CHANGE#, L.NEXT_CHANGE#,
           MIN(F.MEMBER) AS NAME, 2 AS SOURCE_PRIORITY
    FROM V$LOG L
    JOIN V$LOGFILE F ON F.GROUP# = L.GROUP#
    WHERE L.FIRST_CHANGE# <= :end_scn
      AND L.NEXT_CHANGE# > :start_scn
    GROUP BY L.THREAD#, L.SEQUENCE#, L.FIRST_CHANGE#, L.NEXT_CHANGE#
)
SELECT NAME
FROM (
    SELECT C.*,
           ROW_NUMBER() OVER (
               PARTITION BY THREAD#, SEQUENCE#
               ORDER BY SOURCE_PRIORITY, NAME
           ) AS RN
    FROM CANDIDATES C
)
WHERE RN = 1
ORDER BY THREAD#, SEQUENCE#;
```

Archived log được ưu tiên hơn online redo cùng sequence. File đầu được đăng ký bằng
`DBMS_LOGMNR.NEW`, các file sau bằng `DBMS_LOGMNR.ADDFILE`. Tên filesystem, FRA hay ASM
đều do Oracle trả về; application không cần biết storage layout của UAT.

Khi connector có `database.pdb.name`, service giữ LogMiner connection ở `CDB$ROOT`, tự
prefix tên cột mining bằng PDB và chỉ switch container khi đọc metadata/Flashback source.
`ORACLE_DSN` vì vậy phải trỏ tới CDB root, không trỏ trực tiếp PDB.

Nếu không còn đủ redo hoặc Oracle trả `ORA-01291`, transaction thất bại và escalation;
không publish một phần.

## 4. Mining đúng từng DML

Sau khi đăng ký file, service mở LogMiner trong cùng connection:

```sql
BEGIN
    DBMS_LOGMNR.START_LOGMNR(
        STARTSCN => :start_scn,
        ENDSCN   => :commit_scn,
        OPTIONS  => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                  + DBMS_LOGMNR.COMMITTED_DATA_ONLY
    );
END;
```

Mỗi logical row change được đọc từ `V$LOGMNR_CONTENTS`, lọc đúng XID và sắp xếp:

```sql
SELECT
    SCN, RS_ID, SSN, CSF, SEQUENCE#,
    RAWTOHEX(XID) AS XID_HEX,
    SEG_OWNER AS TABLE_OWNER, TABLE_NAME, ROW_ID,
    OPERATION, SQL_REDO, SQL_UNDO, COMMIT_SCN, TIMESTAMP,
    START_TIMESTAMP, COMMIT_TIMESTAMP, THREAD#, USERNAME
FROM V$LOGMNR_CONTENTS
WHERE XID = HEXTORAW(:transaction_id)
  AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
ORDER BY SEQUENCE#, SCN, RS_ID, SSN;
```

`SEQUENCE#` giữ thứ tự SQL trong transaction. `SCN, RS_ID, SSN` chỉ làm tie-breaker vì
nhiều DML có thể cùng SCN, còn thứ tự vật lý `RS_ID` không luôn trùng thứ tự nghiệp vụ,
đặc biệt với chuỗi insert-update-delete trên cùng row.

Service ghép liên tiếp `SQL_REDO` và `SQL_UNDO` khi `CSF=1`, sau đó parse câu SQL theo
metadata động của bảng; các câu SQL này chỉ được đọc, không bao giờ được execute:

- `INSERT`: lấy `after` từ column/value list của `SQL_REDO`.
- `UPDATE`: lấy giá trị cũ từ `WHERE` của `SQL_REDO` và `SET` của `SQL_UNDO`; lấy giá
  trị mới từ `SET` của `SQL_REDO` và `WHERE` của `SQL_UNDO`.
- `DELETE`: lấy `before` từ câu `INSERT` trong `SQL_UNDO`; nếu không có `SQL_UNDO` thì
  fallback sang `WHERE` của `SQL_REDO`.
- Bỏ predicate `ROWID` khi dựng key; primary key được xác định theo metadata của bảng.
- Nếu literal/hàm Oracle không được parser hỗ trợ, statement thiếu fragment hoặc thiếu
  primary key, transaction fail-closed và không phát repair message.

ROWID được chuyển động bằng `DBMS_ROWID` về dạng Debezium (data-object number bằng 0),
không phụ thuộc tên bảng hay physical object id của môi trường. Ví dụ physical ROWID
`AAAR6dAAHAAAAGuAAH` được emit thành `AAAAAAAAHAAAAGuAAH`.

## 5. Đối chiếu trạng thái hiện tại theo primary key

Redo vẫn được đọc và đối chiếu đủ số DML để không xử lý transaction thiếu dữ liệu. Sau
đó service gom theo `(table, primary key)`, batch các key cùng bảng vào một lần SELECT
source và chỉ đánh giá trạng thái mỗi key một lần:

- Source có row và lịch sử XID có `INSERT`: phát một `c`, `before=null`, `after` là
  toàn bộ row hiện tại.
- Source có row và lịch sử XID chỉ có `UPDATE`: phát một `u`, `before` lấy từ UPDATE
  cuối đã reconstruct, `after` là toàn bộ row hiện tại.
- Source không có row và lịch sử XID có `DELETE`: phát một `d`, `before` lấy từ trạng
  thái trước delete, `after=null`.
- Source có row và lịch sử XID chỉ có `DELETE`: bypass; key đã được nghiệp vụ tạo lại,
  phát delete cũ sẽ làm mất dữ liệu downstream.
- Source không có row và lịch sử XID chỉ có `INSERT/UPDATE`: bypass vì không còn row
  nguồn để dựng.
- Mỗi key phát tối đa một repair message.

`AS OF SCN :start_scn` vẫn được dùng nội bộ khi cần dựng `before` an toàn cho delete;
các key cùng bảng cũng được đọc theo batch. SELECT current state mới là bước quyết định
có phát `c`, `d` hay bypass.

Primary-key update hiện fail-closed vì Debezium có key-change semantics riêng; service
không tự phát message có thể sai. Bảng không có PK, ROW_ID thiếu, type Oracle chưa hỗ trợ,
không dựng đủ ảnh row hoặc Flashback seed hết UNDO đều thất bại và escalation.

## 6. Đối chiếu và publish

Trước khi publish:

```text
số logical DML từ LogMiner
    = tổng CHANGE_COUNT từ FLASHBACK_TRANSACTION_QUERY
```

Sai số lượng dù chỉ một event sẽ dừng toàn bộ XID. Mapping output:

| Oracle | Debezium `op` | `before` | `after` |
| --- | --- | --- | --- |
| Source có row, XID có I/U | `c` | `null` | row hiện tại từ source |
| Source empty, XID có D | `d` | row trước delete | `null` |

Message được publish theo thứ tự key xuất hiện đầu tiên vào topic
`{topic.prefix}.{schema đã adjust}.{table}` trong một Kafka transaction. Nếu connector
bật `provide.transaction.metadata`, service thêm `total_order` và
`data_collection_order`; nếu không, trường `transaction` giữ `null` giống connector.

### Contract payload động

Payload không lấy cấu trúc từ một bảng mẫu. Với mỗi connector/table, service:

- lấy connector version từ Kafka Connect `/status`;
- lấy `database.dbname`, `database.pdb.name`, `decimal.handling.mode`,
  `time.precision.mode`, `provide.transaction.metadata` và
  `log.mining.include.redo.sql` từ config runtime;
- lấy toàn bộ column/type/scale, nullable/default và primary key từ Oracle dictionary;
- serialize `NUMBER` theo runtime `decimal.handling.mode`: `string` thành JSON string,
  `double` thành JSON number;
- serialize Oracle date/timestamp thành epoch milliseconds vì connector yêu cầu
  `time.precision.mode=connect`, RAW thành Base64;
- tạo Kafka key từ toàn bộ PK theo đúng thứ tự constraint. Với
  `key.converter.schemas.enable=true`, key có envelope `{schema, payload}`, tên schema
  là `{topic.prefix}.{schema}.{table}.Key` và `schema.fields` được dựng động từ metadata;
  với `false`, key là JSON phẳng;
- phát đầy đủ `source.version`, `snapshot="false"`, `sequence`, `ts_ms/us/ns`, `txId`,
  `scn`, `commit_scn`, `lcr_position`, `rs_id`, `ssn`, `redo_thread`, `user_name`,
  `redo_sql`, `row_id`, `commit_ts_ms`, `start_scn`, `start_ts_ms`, `txSeq`;
- phát đủ top-level `ts_ms`, `ts_us`, `ts_ns`.

Kafka headers của repair message giữ cùng context contract mà downstream đang dùng:

```json
{
  "__debezium.context.connectorName": "oracle",
  "__debezium.context.connectorLogicalName": "CDC.TOPO-CLI",
  "__debezium.context.taskId": "0",
  "__debezium.context.runId": "019f6368-c503-7d1e-8d07-3ff4d6dafdd1"
}
```

`connectorLogicalName` lấy từ `topic.prefix`, `taskId` lấy từ Kafka Connect status.
`runId` ưu tiên context do webhook truyền xuống; nếu nguồn alert không có trường này,
service dùng UUIDv7 ổn định trong vòng đời instance. Không thêm field remediation riêng
vào `source` để tránh phá strict DTO của bên thứ ba. Kiểu Oracle hoặc connector contract
chưa hỗ trợ sẽ fail-closed trước khi publish.

Hệ thống stateless và chấp nhận cùng repair XID có thể được phát lại. Không có Oracle
side table; XID/SCN phục vụ audit nằm trong payload `source`.

## 7. Vòng đời và điều kiện UAT

Mỗi XID/batch dùng một Oracle connection theo vòng đời:

```text
discover logs → ADD_LOGFILE → START_LOGMNR → query XID → END_LOGMNR
```

`END_LOGMNR` luôn chạy trong `finally`; connection đang mining không được trả lại pool.
DBA cấp quyền trong `CDB$ROOT` cho common remediation user. Ngoài quyền
LogMiner/`SET CONTAINER`, user vẫn cần `SELECT ANY TRANSACTION`, quyền đọc dictionary,
`SELECT` và `FLASHBACK` trên các source table.

UAT phải giữ online/archive redo lâu hơn độ trễ alert + retry. Nếu redo đã bị overwrite,
xóa hoặc không còn truy cập được thì không thể replay chính xác. Direct replay là event
đến muộn trên Kafka; downstream cần tôn trọng `source.scn/commit_scn` nếu có thể nhận event
mới hơn trước repair.
