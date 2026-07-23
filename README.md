# Khôi phục transaction bị Debezium Oracle abandoned

## Bài toán nghiệp vụ

Khi Debezium Oracle giữ một transaction quá thời gian retention, transaction có thể bị
đánh dấu `abandoned`. Các thay đổi thuộc transaction đó không được phát đầy đủ lên Kafka,
gây thiếu dữ liệu CDC ở hệ thống downstream.

Hệ thống remediation có nhiệm vụ nhận diện transaction bị mất, lấy các primary key bị
ảnh hưởng, đối chiếu trạng thái hiện tại ở Oracle và phát message để downstream hội tụ
về source. Mỗi `(table, primary key)` được phát tối đa một repair message.

App chỉ đọc Oracle và phát repair message lên Kafka. App tuyệt đối không thực thi
`INSERT`, `UPDATE`, `DELETE`, `MERGE` hoặc DDL lên database nguồn hay bất kỳ database
đích nào. Các script game-day tạo DML chỉ là công cụ kiểm thử độc lập, không thuộc luồng
runtime của app.

Phạm vi chính là các sự cố có ít record trên mỗi transaction. Cùng một repair message có
thể được phát trùng; downstream cần xử lý idempotent theo XID/SCN khi cần.

Tài liệu nghiệp vụ chi tiết và SSOT của hệ thống nằm tại
[BUSINESS_LOGIC.md](BUSINESS_LOGIC.md).

## Đầu vào

Đầu vào là toàn bộ dòng log abandoned, tối thiểu chứa:

- tên Kafka Connect connector;
- Oracle transaction ID dạng RAW(8), biểu diễn bằng 16 ký tự hex;
- start SCN;
- thời điểm thay đổi;
- redo thread;
- số event của transaction;
- toàn bộ dòng log gốc để phục vụ đối soát.

Ví dụ:

```text
Transaction 02001600a8030000 (start SCN 6178327, change time 2026-07-21T03:06:20Z, redo thread 1, 5 events) is being abandoned.
```

Transaction có `0 events` được bypass ngay, không tạo request remediation và không truy
vấn Oracle.

## Luồng xử lý nghiệp vụ

1. Nhận log abandoned và đưa request chứa đầy đủ thông tin transaction vào Kafka.
2. Đọc cấu hình connector để xác định các bảng thực sự thuộc phạm vi CDC.
3. Tra cứu transaction trong Oracle để xác định bảng, số DML, start SCN và commit SCN.
4. Bỏ qua các bảng không thuộc phạm vi connector.
5. Đọc redo/archive log bao phủ transaction, lấy đủ DML theo thứ tự
   `SEQUENCE#, SCN, RS_ID, SSN` và gom theo primary key.
6. Gom các key theo bảng và SELECT trạng thái hiện tại theo batch; mỗi key chỉ được đối
   chiếu một lần để quyết định repair.
7. Đối chiếu tổng số DML lấy được với transaction metadata. Nếu thiếu bất kỳ event nào,
   toàn bộ XID thất bại và không được phát bù một phần.
8. Chuyển operation Oracle sang Debezium operation và phát vào đúng CDC topic.
9. Chỉ xác nhận request hoàn tất sau khi toàn bộ repair message đã được phát thành công.

## Quy tắc dựng event

| Oracle operation | Debezium `op` | `before` | `after` |
|---|---|---|---|
| Có row hiện tại và XID có `INSERT/UPDATE` | `c` | `null` | Toàn bộ row hiện tại |
| Không có row hiện tại và XID có `DELETE` | `d` | Row trước delete | `null` |
| Có row hiện tại và XID chỉ có `DELETE` | Bypass | — | Tránh xóa row đã được tạo lại |
| Không có row hiện tại và XID chỉ có `INSERT/UPDATE` | Bypass | — | Không có source row để dựng |

Chuỗi nhiều DML cùng key được gom thành một quyết định theo source hiện tại. Ví dụ
`INSERT → UPDATE → DELETE` với source hiện không còn row chỉ phát một `d`; `DELETE →
INSERT` với source đang có row chỉ phát một `c`.

## Kết quả đầu ra

Repair message được phát vào topic:

```text
{topic.prefix}.{schema}.{table}
```

Kafka key được dựng động từ toàn bộ primary key của bảng. Nếu connector bật
`key.converter.schemas.enable=true`, key giữ đúng envelope `{schema, payload}` của
Kafka Connect; `schema.fields` chỉ chứa các cột PK theo đúng thứ tự constraint.
Nếu cấu hình là `false`, key là JSON phẳng. Payload giữ contract Debezium gồm
`before`, `after`, `source`, `transaction`, `op` và các processing timestamp.

Kafka headers giữ context downstream yêu cầu:

```json
{
  "__debezium.context.connectorName": "oracle",
  "__debezium.context.connectorLogicalName": "CDC.TOPO-CLI",
  "__debezium.context.taskId": "0",
  "__debezium.context.runId": "019f6368-c503-7d1e-8d07-3ff4d6dafdd1"
}
```

## Điều kiện không được phát repair

Transaction không được phát bù khi xảy ra một trong các trường hợp:

- transaction có `0 events`;
- transaction chưa commit hoặc đã rollback;
- không còn đủ redo/archive log;
- không còn dữ liệu Flashback cần thiết để dựng trạng thái trước transaction;
- số DML đọc được không khớp transaction metadata;
- bảng không có primary key;
- không thể dựng chính xác `before`, `after`, key hoặc thứ tự operation;
- contract connector không thể được tái tạo an toàn.

## Giới hạn nghiệp vụ

- Repair event luôn đến muộn hơn CDC event thông thường. Downstream cần xét SCN nếu có
  khả năng event mới hơn đã được xử lý trước repair.
- Hệ thống chấp nhận phát trùng repair message và không lưu side table chống trùng.
- Khi redo/archive hoặc UNDO cần thiết đã bị xóa, transaction không thể được khôi phục
  chính xác và phải chuyển sang xử lý thủ công.
