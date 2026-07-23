from __future__ import annotations

from contextlib import contextmanager
from collections import defaultdict
from decimal import Decimal
from threading import Lock
from time import monotonic
from typing import Any, Callable, Iterator, Mapping, Sequence

import oracledb

from remediation.models import (
    TableMetadata,
    TableRef,
    TransactionTable,
    MinedChange,
)
from remediation.logminer_sql_parser import (
    LogMinerSqlParseError,
    parse_logminer_change,
)
from remediation.sql_utils import quote_identifier, quote_qualified_name


def _row_to_dict(cursor: oracledb.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {description[0]: value for description, value in zip(cursor.description, row)}


class OracleClient:
    TABLE_METADATA_CACHE_TTL_SECONDS = 600.0

    @staticmethod
    def _initialize_session(
        connection: oracledb.Connection, _requested_tag: str | None
    ) -> None:
        with connection.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
            cursor.execute("ALTER SESSION SET TIME_ZONE = 'UTC'")
            cursor.execute(
                "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            )
            cursor.execute(
                "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = "
                "'YYYY-MM-DD HH24:MI:SS.FF9'"
            )
            cursor.execute(
                "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = "
                "'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'"
            )

    def __init__(
        self,
        user: str,
        password: str,
        dsn: str,
        pool_min: int,
        pool_max: int,
        query_observer: Callable[[str, str, Mapping[str, Any]], None] | None = None,
    ) -> None:
        self.pool = oracledb.create_pool(
            user=user,
            password=password,
            dsn=dsn,
            min=pool_min,
            max=pool_max,
            increment=1,
            getmode=oracledb.POOL_GETMODE_WAIT,
            session_callback=self._initialize_session,
        )
        self._table_metadata_cache: dict[
            tuple[str, str, str], tuple[float, TableMetadata]
        ] = {}
        self._table_metadata_cache_lock = Lock()
        self._table_metadata_load_locks: dict[tuple[str, str, str], Lock] = {}
        self._query_observer = query_observer

    def _observe_query(
        self, stage: str, sql: str, binds: Mapping[str, Any] | None = None
    ) -> None:
        if self._query_observer is not None:
            self._query_observer(stage, sql.strip(), dict(binds or {}))

    @contextmanager
    def connection(self) -> Iterator[oracledb.Connection]:
        with self.pool.acquire() as connection:
            yield connection

    @staticmethod
    def _current_container(cursor: oracledb.Cursor) -> str:
        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM DUAL")
        return str(cursor.fetchone()[0])

    @staticmethod
    def _set_container(cursor: oracledb.Cursor, container: str) -> None:
        cursor.execute(f"ALTER SESSION SET CONTAINER = {quote_identifier(container)}")

    def find_transaction_tables(
        self, xid: str, pdb_name: str | None = None
    ) -> list[TransactionTable]:
        sql = """
            SELECT TABLE_OWNER, TABLE_NAME, OPERATION, COUNT(*) AS CHANGE_COUNT,
                   MIN(START_SCN) AS START_SCN,
                   MAX(COMMIT_SCN) AS COMMIT_SCN,
                   MIN(START_TIMESTAMP) AS START_TIME,
                   MAX(COMMIT_TIMESTAMP) AS COMMIT_TIME
            FROM FLASHBACK_TRANSACTION_QUERY
            WHERE XID = HEXTORAW(:xid)
              AND TABLE_OWNER IS NOT NULL
              AND TABLE_NAME IS NOT NULL
              AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
            GROUP BY TABLE_OWNER, TABLE_NAME, OPERATION
            ORDER BY TABLE_OWNER, TABLE_NAME, OPERATION
        """
        with self.connection() as connection, connection.cursor() as cursor:
            original = self._current_container(cursor)
            switched = bool(
                pdb_name and pdb_name.upper() != original.upper()
            )
            try:
                if switched:
                    self._set_container(cursor, pdb_name)
                self._observe_query(
                    "transaction_summary",
                    sql,
                    {"xid": xid},
                )
                cursor.execute(sql, xid=xid)
                return [
                    TransactionTable(
                        table=TableRef(str(row[0]), str(row[1])),
                        change_count=int(row[3]),
                        start_scn=int(row[4]),
                        commit_scn=int(row[5]) if row[5] is not None else None,
                        start_time=row[6],
                        commit_time=row[7],
                    )
                    for row in cursor
                ]
            finally:
                if switched:
                    self._set_container(cursor, original)

    def get_table_metadata(
        self, table: TableRef, pdb_name: str | None = None
    ) -> TableMetadata:
        cache_key = (
            (pdb_name or "").upper(),
            table.owner.upper(),
            table.name.upper(),
        )
        now = monotonic()
        with self._table_metadata_cache_lock:
            cached = self._table_metadata_cache.get(cache_key)
            if cached is not None and cached[0] > now:
                return cached[1]
            load_lock = self._table_metadata_load_locks.setdefault(cache_key, Lock())

        # Collapse concurrent misses for the same table without serializing
        # metadata loads for unrelated tables.
        with load_lock:
            now = monotonic()
            with self._table_metadata_cache_lock:
                cached = self._table_metadata_cache.get(cache_key)
                if cached is not None and cached[0] > now:
                    return cached[1]

            metadata = self._load_table_metadata(table, pdb_name)
            expires_at = monotonic() + self.TABLE_METADATA_CACHE_TTL_SECONDS
            with self._table_metadata_cache_lock:
                self._table_metadata_cache[cache_key] = (expires_at, metadata)
            return metadata

    def _load_table_metadata(
        self, table: TableRef, pdb_name: str | None = None
    ) -> TableMetadata:
        columns_sql = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_SCALE, NULLABLE, DATA_DEFAULT
            FROM ALL_TAB_COLS
            WHERE OWNER = :owner
              AND TABLE_NAME = :table_name
              AND HIDDEN_COLUMN = 'NO'
              AND VIRTUAL_COLUMN = 'NO'
            ORDER BY COLUMN_ID
        """
        keys_sql = """
            SELECT cols.COLUMN_NAME
            FROM ALL_CONSTRAINTS cons
            JOIN ALL_CONS_COLUMNS cols
              ON cols.OWNER = cons.OWNER
             AND cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME
            WHERE cons.OWNER = :owner
              AND cons.TABLE_NAME = :table_name
              AND cons.CONSTRAINT_TYPE = 'P'
            ORDER BY cols.POSITION
        """
        params = {"owner": table.owner, "table_name": table.name}
        with self.connection() as connection, connection.cursor() as cursor:
            original = self._current_container(cursor)
            switched = bool(
                pdb_name and pdb_name.upper() != original.upper()
            )
            try:
                if switched:
                    self._set_container(cursor, pdb_name)
                cursor.execute(columns_sql, params)
                column_rows = [
                    (
                        str(row[0]),
                        str(row[1]),
                        int(row[2]) if row[2] is not None else None,
                        str(row[3]).upper() == "Y",
                        str(row[4]).strip() if row[4] is not None else None,
                    )
                    for row in cursor
                ]
                cursor.execute(keys_sql, params)
                key_columns = tuple(str(row[0]) for row in cursor)
            finally:
                if switched:
                    self._set_container(cursor, original)
        columns = tuple(row[0] for row in column_rows)
        if not columns:
            raise ValueError(f"Table not found or not visible: {table.qualified_name}")
        if not key_columns:
            raise ValueError(
                f"Table {table.qualified_name} has no primary key; automatic repair is unsafe"
            )
        return TableMetadata(
            table=table,
            columns=columns,
            key_columns=key_columns,
            column_types={row[0]: row[1] for row in column_rows},
            column_scales={row[0]: row[2] for row in column_rows},
            column_nullable={row[0]: row[3] for row in column_rows},
            column_defaults={row[0]: row[4] for row in column_rows},
        )

    @staticmethod
    def _end_logminer(
        cursor: oracledb.Cursor, *, ignore_not_started: bool = False
    ) -> None:
        try:
            cursor.callproc("DBMS_LOGMNR.END_LOGMNR")
        except oracledb.DatabaseError as exc:
            error = exc.args[0]
            if ignore_not_started and getattr(error, "code", None) == 1307:
                return
            raise

    def _discover_logfiles(
        self,
        cursor: oracledb.Cursor, start_scn: int, end_scn: int
    ) -> list[str]:
        sql = """
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
            ), RANKED AS (
                SELECT C.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY THREAD#, SEQUENCE#
                           ORDER BY SOURCE_PRIORITY, NAME
                       ) AS RN
                FROM CANDIDATES C
            )
            SELECT NAME
            FROM RANKED
            WHERE RN = 1
            ORDER BY THREAD#, SEQUENCE#
        """
        binds = {"start_scn": start_scn, "end_scn": end_scn}
        self._observe_query("logminer_logfile_discovery", sql, binds)
        cursor.execute(sql, **binds)
        return [str(row[0]) for row in cursor]

    def _add_logfiles(
        self, cursor: oracledb.Cursor, logfiles: Sequence[str]
    ) -> None:
        if not logfiles:
            raise RuntimeError("No online redo or archived log covers the requested SCN range")
        for index, logfile in enumerate(logfiles):
            option = "DBMS_LOGMNR.NEW" if index == 0 else "DBMS_LOGMNR.ADDFILE"
            sql = f"""
                BEGIN
                    DBMS_LOGMNR.ADD_LOGFILE(
                        LOGFILENAME => :logfile,
                        OPTIONS => {option}
                    );
                END;
                """
            self._observe_query(
                "logminer_add_logfile",
                sql,
                {"logfile": logfile},
            )
            cursor.execute(sql, logfile=logfile)

    def _start_logminer(
        self,
        cursor: oracledb.Cursor, start_scn: int, end_scn: int
    ) -> None:
        sql = """
            BEGIN
                DBMS_LOGMNR.START_LOGMNR(
                    STARTSCN => :start_scn,
                    ENDSCN => :end_scn,
                    OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                             + DBMS_LOGMNR.COMMITTED_DATA_ONLY
                );
            END;
            """
        binds = {"start_scn": start_scn, "end_scn": end_scn}
        self._observe_query("logminer_start", sql, binds)
        cursor.execute(sql, **binds)

    @staticmethod
    def _normalize_number(value: Any, scale: int | None) -> str | None:
        if value is None:
            return None
        decimal_value = Decimal(str(value).strip())
        if scale is None:
            return format(decimal_value, "f")
        if scale >= 0:
            return f"{decimal_value:.{scale}f}"
        quantum = Decimal(1).scaleb(-scale)
        return format(decimal_value.quantize(quantum), "f")

    def _mine_sql_statements(
        self,
        cursor: oracledb.Cursor,
        xid: str,
        metadata_by_table: dict[TableRef, TableMetadata],
    ) -> list[MinedChange]:
        sql = """
                SELECT SCN, RS_ID, SSN, CSF, SEQUENCE# AS TX_SEQUENCE,
                       RAWTOHEX(XID) AS XID_HEX,
                       SEG_OWNER AS TABLE_OWNER, TABLE_NAME, ROW_ID,
                       CASE
                            WHEN ROW_ID IS NULL THEN NULL
                           ELSE DBMS_ROWID.ROWID_CREATE(
                               1,
                               0,
                               DBMS_ROWID.ROWID_RELATIVE_FNO(CHARTOROWID(ROW_ID)),
                               DBMS_ROWID.ROWID_BLOCK_NUMBER(CHARTOROWID(ROW_ID)),
                               DBMS_ROWID.ROWID_ROW_NUMBER(CHARTOROWID(ROW_ID))
                            )
                        END AS DEBEZIUM_ROW_ID,
                       OPERATION, SQL_REDO, SQL_UNDO, COMMIT_SCN, TIMESTAMP,
                       START_TIMESTAMP, COMMIT_TIMESTAMP,
                       THREAD#, USERNAME
                FROM V$LOGMNR_CONTENTS
                WHERE XID = HEXTORAW(:xid)
                  AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
                ORDER BY SEQUENCE#, SCN, RS_ID, SSN
        """
        binds = {"xid": xid}
        self._observe_query("logminer_dml", sql, binds)
        cursor.execute(sql, **binds)
        result: list[MinedChange] = []
        pending: dict[str, Any] | None = None

        def finish(item: dict[str, Any]) -> None:
            table = TableRef(item["owner"], item["table_name"])
            metadata = metadata_by_table.get(table)
            if metadata is None:
                return
            redo_sql = "".join(item["redo_fragments"]) or None
            undo_sql = "".join(item["undo_fragments"]) or None
            try:
                before_delta, after_delta = parse_logminer_change(
                    item["operation"],
                    redo_sql,
                    undo_sql,
                    metadata,
                )
            except LogMinerSqlParseError as exc:
                raise LogMinerSqlParseError(
                    f"{table.qualified_name} SCN={item['scn']} "
                    f"RS_ID={item['rs_id']} SSN={item['ssn']}: {exc}"
                ) from exc
            result.append(
                MinedChange(
                    table=table,
                    operation=item["operation"],
                    scn=item["scn"],
                    commit_scn=item["commit_scn"],
                    rs_id=item["rs_id"],
                    ssn=item["ssn"],
                    row_id=item["row_id"],
                    redo_thread=item["redo_thread"],
                    user_name=item["user_name"],
                    change_time=item["change_time"],
                    start_time=item["start_time"],
                    commit_time=item["commit_time"],
                    redo_sql=redo_sql,
                    undo_sql=undo_sql,
                    before_delta=before_delta,
                    after_delta=after_delta,
                )
            )

        for row in cursor:
            if pending is None:
                pending = {
                    "scn": int(row[0]),
                    "rs_id": str(row[1]).strip(),
                    "ssn": int(row[2]),
                    "sequence": int(row[4]),
                    "owner": str(row[6]),
                    "table_name": str(row[7]),
                    "row_id": str(row[9]) if row[9] else None,
                    "operation": str(row[10]),
                    "commit_scn": int(row[13]),
                    "change_time": row[14],
                    "start_time": row[15],
                    "commit_time": row[16],
                    "redo_thread": int(row[17]) if row[17] is not None else None,
                    "user_name": str(row[18]) if row[18] else None,
                    "redo_fragments": [],
                    "undo_fragments": [],
                }
            if row[11] is not None:
                pending["redo_fragments"].append(str(row[11]))
            if row[12] is not None:
                pending["undo_fragments"].append(str(row[12]))
            if int(row[3] or 0) == 0:
                finish(pending)
                pending = None
        if pending is not None:
            raise LogMinerSqlParseError(
                "Last V$LOGMNR_CONTENTS row has CSF=1; SQL statement is incomplete"
            )
        return result

    def mine_transaction(
        self,
        xid: str,
        metadata_by_table: dict[TableRef, TableMetadata],
        start_scn: int,
        end_scn: int,
        pdb_name: str | None = None,
        include_redo_sql: bool = False,
    ) -> list[MinedChange]:
        with self.connection() as connection, connection.cursor() as cursor:
            container = self._current_container(cursor)
            if pdb_name and container.upper() != "CDB$ROOT":
                raise RuntimeError(
                    "LogMiner connection must target CDB$ROOT when database.pdb.name is configured"
                )
            try:
                logfiles = self._discover_logfiles(cursor, start_scn, end_scn)
                self._add_logfiles(cursor, logfiles)
                self._start_logminer(cursor, start_scn, end_scn)

                return self._mine_sql_statements(
                    cursor,
                    xid,
                    metadata_by_table,
                )
            finally:
                self._end_logminer(cursor, ignore_not_started=True)

    def _normalize_row(
        self, metadata: TableMetadata, values: dict[str, Any]
    ) -> dict[str, Any]:
        for column, value in values.items():
            if (
                value is not None
                and metadata.column_types[column].upper() in {"NUMBER", "FLOAT"}
            ):
                values[column] = self._normalize_number(
                    value, metadata.column_scales[column]
                )
        return values

    def _get_rows(
        self,
        metadata: TableMetadata,
        keys: Sequence[dict[str, Any]],
        *,
        scn: int | None,
        pdb_name: str | None,
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        if not keys:
            return {}

        column_sql = ", ".join(quote_identifier(c) for c in metadata.columns)
        table_sql = quote_qualified_name(metadata.table.qualified_name)
        unique_keys: dict[tuple[Any, ...], dict[str, Any]] = {}
        for key in keys:
            identity = tuple(key[column] for column in metadata.key_columns)
            unique_keys.setdefault(identity, key)

        key_predicates: list[str] = []
        binds: dict[str, Any] = {}
        for key_index, key in enumerate(unique_keys.values()):
            predicates: list[str] = []
            for column_index, column in enumerate(metadata.key_columns):
                bind_name = f"key_{key_index}_{column_index}"
                predicates.append(f"{quote_identifier(column)} = :{bind_name}")
                binds[bind_name] = key[column]
            key_predicates.append(f"({' AND '.join(predicates)})")

        flashback_sql = ""
        if scn is not None:
            flashback_sql = " AS OF SCN :as_of_scn"
            binds["as_of_scn"] = scn
        sql = f"""
            SELECT {column_sql}
            FROM {table_sql}{flashback_sql}
            WHERE {' OR '.join(key_predicates)}
        """
        stage = "source_as_of" if scn is not None else "source_current"
        self._observe_query(stage, sql, binds)

        with self.connection() as connection, connection.cursor() as cursor:
            original = self._current_container(cursor)
            switched = bool(
                pdb_name and pdb_name.upper() != original.upper()
            )
            try:
                if switched:
                    self._set_container(cursor, pdb_name)
                cursor.execute(sql, binds)
                result: dict[tuple[Any, ...], dict[str, Any]] = {}
                for row in cursor:
                    values = self._normalize_row(
                        metadata, _row_to_dict(cursor, row)
                    )
                    identity = tuple(
                        values[column] for column in metadata.key_columns
                    )
                    result[identity] = values
                return result
            finally:
                if switched:
                    self._set_container(cursor, original)

    def get_rows_as_of(
        self,
        metadata: TableMetadata,
        keys: Sequence[dict[str, Any]],
        scn: int,
        pdb_name: str | None = None,
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        return self._get_rows(
            metadata, keys, scn=scn, pdb_name=pdb_name
        )

    def get_row_as_of(
        self,
        metadata: TableMetadata,
        key: dict[str, Any],
        scn: int,
        pdb_name: str | None = None,
    ) -> dict[str, Any] | None:
        identity = tuple(key[column] for column in metadata.key_columns)
        return self.get_rows_as_of(metadata, [key], scn, pdb_name).get(identity)

    def get_current_rows(
        self,
        metadata: TableMetadata,
        keys: Sequence[dict[str, Any]],
        pdb_name: str | None = None,
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        return self._get_rows(
            metadata, keys, scn=None, pdb_name=pdb_name
        )

    def get_current_row(
        self,
        metadata: TableMetadata,
        key: dict[str, Any],
        pdb_name: str | None = None,
    ) -> dict[str, Any] | None:
        identity = tuple(key[column] for column in metadata.key_columns)
        return self.get_current_rows(metadata, [key], pdb_name).get(identity)

    def close(self) -> None:
        self.pool.close(force=True)
