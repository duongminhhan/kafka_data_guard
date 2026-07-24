from __future__ import annotations

import re
import time
from collections import defaultdict
from dataclasses import replace
from datetime import timezone
from typing import Any, Callable

from remediation.connect.client import ConnectorRuntimeConfig
from remediation.domain.models import (
    AlertEvent,
    MinedChange,
    RepairRecord,
    ReplayEvent,
    TableMetadata,
    TableRef,
    TransactionTable,
)
from remediation.oracle.client import OracleClient
from remediation.application.event_reconstructor import reconstruct_events


class FlashbackDataMissingError(RuntimeError):
    pass


class TransactionNotCommittedError(RuntimeError):
    pass


class IncompleteTransactionError(RuntimeError):
    pass


def adjust_topic_component(value: str) -> str:
    """Match Kafka Connect's replacement of invalid topic-name characters."""
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def _source(
    event: AlertEvent,
    replay: ReplayEvent,
    connector_config: ConnectorRuntimeConfig,
    start_scn: int,
    start_time_ms: int | None,
    commit_time_ms: int | None,
) -> dict[str, Any]:
    change_time = replay.change_time
    if change_time is not None and change_time.tzinfo is None:
        change_time = change_time.replace(tzinfo=timezone.utc)
    source_time_ms = int(change_time.timestamp() * 1000) if change_time else None
    mined_start_time_ms = _epoch_ms(replay.start_time)
    mined_commit_time_ms = _epoch_ms(replay.commit_time)
    return {
        "version": connector_config.connector_version,
        "connector": "oracle",
        "name": connector_config.topic_prefix,
        "ts_ms": source_time_ms,
        "snapshot": "false",
        "db": connector_config.database_name,
        "sequence": None,
        "ts_us": source_time_ms * 1_000 if source_time_ms is not None else None,
        "ts_ns": source_time_ms * 1_000_000 if source_time_ms is not None else None,
        "schema": replay.table.owner,
        "table": replay.table.name,
        "txId": event.xid.lower(),
        "scn": str(replay.scn),
        "commit_scn": str(replay.commit_scn),
        "lcr_position": None,
        "rs_id": replay.rs_id,
        "ssn": replay.ssn,
        "redo_thread": replay.redo_thread,
        "user_name": replay.user_name,
        "redo_sql": replay.redo_sql if connector_config.include_redo_sql else None,
        "row_id": replay.row_id,
        "commit_ts_ms": mined_commit_time_ms or commit_time_ms,
        "start_scn": str(start_scn),
        "start_ts_ms": mined_start_time_ms or start_time_ms,
        "txSeq": replay.order,
    }


def _epoch_ms(value: Any) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _convert_number(value: Any, decimal_mode: str) -> Any:
    if value is None:
        return None
    return float(value) if decimal_mode == "double" else str(value)


def _convert_row(
    row: dict[str, Any] | None,
    metadata: Any,
    connector_config: ConnectorRuntimeConfig,
) -> dict[str, Any] | None:
    if row is None:
        return None
    converted = dict(row)
    for column, value in converted.items():
        if metadata.column_types[column].upper() in {"NUMBER", "DECIMAL", "NUMERIC"}:
            converted[column] = _convert_number(
                value, connector_config.decimal_handling_mode
            )
    return converted


def _key_field_schema(
    column: str,
    metadata: Any,
    connector_config: ConnectorRuntimeConfig,
) -> dict[str, Any]:
    oracle_type = metadata.column_types[column].upper()
    if oracle_type in {"NUMBER", "DECIMAL", "NUMERIC"}:
        field_type = (
            "double" if connector_config.decimal_handling_mode == "double" else "string"
        )
    elif oracle_type == "BINARY_FLOAT":
        field_type = "float"
    elif oracle_type == "BINARY_DOUBLE":
        field_type = "double"
    elif oracle_type in {"RAW", "BLOB"}:
        field_type = "bytes"
    else:
        field_type = "string"
    field: dict[str, Any] = {
        "type": field_type,
        "optional": metadata.column_nullable.get(column, False),
        "field": column,
    }
    default = metadata.column_defaults.get(column)
    if default is not None:
        parsed = _parse_key_default(default, field_type)
        if parsed is not None:
            field["default"] = parsed
    return field


def _parse_key_default(default: str, field_type: str) -> Any:
    value = default.strip()
    if value.upper() == "NULL":
        return None
    quoted = re.fullmatch(r"'(.*)'", value, flags=re.DOTALL)
    if quoted:
        value = quoted.group(1).replace("''", "'")
    if field_type in {"float", "double"}:
        try:
            return float(value)
        except ValueError:
            return None
    if field_type == "string" and quoted:
        return value
    return None


def _message_key(
    replay: ReplayEvent,
    metadata: Any,
    connector_config: ConnectorRuntimeConfig,
) -> dict[str, Any]:
    payload = _convert_row(replay.key, metadata, connector_config) or {}
    if not connector_config.key_schemas_enabled:
        return payload
    schema_name = ".".join(
        (
            connector_config.topic_prefix,
            adjust_topic_component(replay.table.owner),
            adjust_topic_component(replay.table.name),
            "Key",
        )
    )
    return {
        "schema": {
            "type": "struct",
            "fields": [
                _key_field_schema(column, metadata, connector_config)
                for column in metadata.key_columns
            ],
            "optional": False,
            "name": schema_name,
        },
        "payload": payload,
    }


Row = dict[str, Any]
RowIdentity = tuple[Any, ...]
RowsByTable = dict[TableRef, dict[RowIdentity, Row]]
GroupedEvents = dict[tuple[TableRef, RowIdentity], list[ReplayEvent]]
RepairDecision = tuple[ReplayEvent, str]


class Reconciler:
    """
    Điều phối remediation theo đúng thứ tự đọc trong build_repairs().

    Các hàm private bên dưới cũng được sắp theo đúng thứ tự gọi:
    phạm vi transaction -> seed rows -> current rows -> quyết định -> Kafka record.
    """

    def __init__(
        self,
        oracle: OracleClient,
        trace_observer: Callable[[str, Any], None] | None = None,
    ) -> None:
        self._oracle = oracle
        self._trace_observer = trace_observer

    def _trace(self, stage: str, value: Any) -> None:
        if self._trace_observer is not None:
            self._trace_observer(stage, value)

    # BẮT ĐẦU ĐỌC BUSINESS LOGIC TỪ HÀM NÀY.
    def build_repairs(
        self,
        event: AlertEvent,
        connector_config: ConnectorRuntimeConfig,
    ) -> tuple[list[RepairRecord], dict[str, int]]:
        """Luồng chính: đọc từ BƯỚC 1 đến BƯỚC 7."""

        # BƯỚC 1: Tìm table, SCN và metadata của transaction.
        included, by_table = self._find_transaction_scope(event, connector_config)
        if not by_table:
            return [], {"create": 0, "update": 0, "delete": 0, "tables": 0}

        metadata_by_table = {
            table: self._oracle.get_table_metadata(table, connector_config.pdb_name)
            for table in by_table
        }
        start_scn, commit_scn, start_time_ms, commit_time_ms = (
            self._transaction_bounds(included)
        )

        # BƯỚC 2: Đọc và parse DML theo đúng thứ tự từ LogMiner.
        mined = self._oracle.mine_transaction(
            event.xid,
            metadata_by_table,
            start_scn,
            commit_scn,
            connector_config.pdb_name,
            connector_config.include_redo_sql,
        )
        self._trace("mined_dml", mined)
        self._validate_mined_count(mined, included)

        # BƯỚC 3: SELECT AS OF SCN để lấy full row trước transaction.
        seed_rows = self._load_seed_rows(
            mined,
            metadata_by_table,
            start_scn,
            connector_config.pdb_name,
        )
        self._trace("source_as_of_rows", seed_rows)

        def seed_loader(metadata: Any, key: Row) -> Row | None:
            identity = self._row_identity(key, metadata.key_columns)
            return seed_rows.get(metadata.table, {}).get(identity)

        # BƯỚC 4: Ghép seed row với delta để dựng event I/U/D đầy đủ.
        replay_events = reconstruct_events(mined, metadata_by_table, seed_loader)
        self._trace("reconstructed_events", replay_events)

        # BƯỚC 5: Gom theo PK và SELECT lại trạng thái hiện tại dưới nguồn.
        grouped = self._group_events_by_primary_key(replay_events, metadata_by_table)
        current_rows = self._load_current_rows(
            grouped,
            metadata_by_table,
            connector_config.pdb_name,
        )
        self._trace("source_current_rows", current_rows)

        # BƯỚC 6: So sánh history với source hiện tại để chọn c/d/bypass.
        decisions, decision_trace = self._choose_repairs(
            grouped,
            current_rows,
            metadata_by_table,
        )
        self._trace("decisions", decision_trace)

        # BƯỚC 7: Dựng Kafka record; service chính sẽ publish sau khi hàm trả về.
        records, stats = self._build_kafka_records(
            decisions,
            event,
            connector_config,
            metadata_by_table,
            start_scn,
            start_time_ms,
            commit_time_ms,
            len(by_table),
        )
        self._trace("repair_records", records)
        self._trace("stats", stats)
        return records, stats

    def _find_transaction_scope(
        self,
        event: AlertEvent,
        connector_config: ConnectorRuntimeConfig,
    ) -> tuple[list[TransactionTable], dict[TableRef, list[TransactionTable]]]:
        """Bước 1: lọc các table thuộc phạm vi connector."""
        summaries = self._oracle.find_transaction_tables(
            event.xid, connector_config.pdb_name
        )
        self._trace("transaction_summaries", summaries)
        if not summaries:
            raise FlashbackDataMissingError(
                "FLASHBACK_TRANSACTION_QUERY returned no DML rows"
            )
        if any(summary.commit_scn is None for summary in summaries):
            raise TransactionNotCommittedError(
                "Abandoned transaction has not committed; retry later or confirm rollback"
            )
        if not connector_config.connector_version:
            raise ValueError("Connector version is required for Debezium-compatible replay")

        included = [
            summary
            for summary in summaries
            if connector_config.includes(summary.table)
        ]
        by_table: dict[TableRef, list[TransactionTable]] = defaultdict(list)
        for summary in included:
            by_table[summary.table].append(summary)
        return included, dict(by_table)

    @staticmethod
    def _transaction_bounds(
        included: list[TransactionTable],
    ) -> tuple[int, int, int | None, int | None]:
        """Bước 1: tính khoảng SCN/time bao phủ toàn transaction."""
        start_scn = min(item.start_scn for item in included)
        commit_scn = max(int(item.commit_scn or 0) for item in included)
        start_times = [
            item.start_time for item in included if item.start_time is not None
        ]
        commit_times = [
            item.commit_time for item in included if item.commit_time is not None
        ]
        start_time_ms = _epoch_ms(min(start_times)) if start_times else None
        commit_time_ms = _epoch_ms(max(commit_times)) if commit_times else None
        return start_scn, commit_scn, start_time_ms, commit_time_ms

    @staticmethod
    def _validate_mined_count(
        mined: list[MinedChange],
        included: list[TransactionTable],
    ) -> None:
        """Bước 2: không replay nếu LogMiner trả thiếu event."""
        expected_count = sum(item.change_count for item in included)
        if len(mined) != expected_count:
            raise IncompleteTransactionError(
                f"LogMiner returned {len(mined)} logical DML events; expected "
                f"{expected_count} from FLASHBACK_TRANSACTION_QUERY"
            )

    def _load_seed_rows(
        self,
        mined: list[MinedChange],
        metadata_by_table: dict[TableRef, TableMetadata],
        start_scn: int,
        pdb_name: str | None,
    ) -> RowsByTable:
        """Bước 3: lấy full row trước transaction cho UPDATE/DELETE."""
        seed_keys_by_table: dict[
            TableRef, dict[RowIdentity, Row]
        ] = defaultdict(dict)
        for change in mined:
            if change.operation.upper() not in {"UPDATE", "DELETE"}:
                continue
            metadata = metadata_by_table[change.table]
            if not all(
                column in change.before_delta for column in metadata.key_columns
            ):
                continue
            key = {
                column: change.before_delta[column]
                for column in metadata.key_columns
            }
            identity = self._row_identity(key, metadata.key_columns)
            seed_keys_by_table[change.table].setdefault(identity, key)

        seed_rows: RowsByTable = {}
        for table, keys_by_identity in seed_keys_by_table.items():
            metadata = metadata_by_table[table]
            seed_rows[table] = self._oracle.get_rows_as_of(
                metadata,
                list(keys_by_identity.values()),
                start_scn,
                pdb_name,
            )
        return seed_rows

    @staticmethod
    def _group_events_by_primary_key(
        replay_events: list[ReplayEvent],
        metadata_by_table: dict[TableRef, TableMetadata],
    ) -> GroupedEvents:
        """Bước 5: mỗi table + PK chỉ tạo tối đa một repair message."""
        grouped: GroupedEvents = {}
        for replay in replay_events:
            metadata = metadata_by_table[replay.table]
            identity = (
                replay.table,
                Reconciler._row_identity(replay.key, metadata.key_columns),
            )
            grouped.setdefault(identity, []).append(replay)
        return grouped

    def _load_current_rows(
        self,
        grouped: GroupedEvents,
        metadata_by_table: dict[TableRef, TableMetadata],
        pdb_name: str | None,
    ) -> RowsByTable:
        """Bước 5: SELECT một lần mỗi table để đối chiếu source hiện tại."""
        current_keys_by_table: dict[
            TableRef, dict[RowIdentity, Row]
        ] = defaultdict(dict)
        for (table, identity), replays in grouped.items():
            current_keys_by_table[table].setdefault(identity, replays[0].key)

        current_rows: RowsByTable = {}
        for table, keys_by_identity in current_keys_by_table.items():
            current_rows[table] = self._oracle.get_current_rows(
                metadata_by_table[table],
                list(keys_by_identity.values()),
                pdb_name,
            )
        return current_rows

    @staticmethod
    def _choose_repairs(
        grouped: GroupedEvents,
        current_rows: RowsByTable,
        metadata_by_table: dict[TableRef, TableMetadata],
    ) -> tuple[list[RepairDecision], list[dict[str, Any]]]:
        """Bước 6: áp dụng bốn nhánh c/d/bypass."""
        decisions: list[RepairDecision] = []
        decision_trace: list[dict[str, Any]] = []
        for (table, identity), replays in grouped.items():
            metadata = metadata_by_table[replays[0].table]
            current = current_rows.get(table, {}).get(identity)
            upserts = [
                replay
                for replay in replays
                if replay.operation in {"INSERT", "UPDATE"}
            ]
            deletes = [replay for replay in replays if replay.operation == "DELETE"]

            if current is not None:
                # Chỉ có DELETE nhưng source còn row: key đã được tạo lại, phải bypass.
                if not upserts:
                    decision_trace.append(
                        {
                            "table": table.qualified_name,
                            "key": replays[0].key,
                            "source_row": current,
                            "operations": [item.operation for item in replays],
                            "decision": "BYPASS",
                            "reason": "delete key exists in current source",
                        }
                    )
                    continue
                representative = upserts[-1]
                current_key = {
                    column: current[column] for column in metadata.key_columns
                }
                decisions.append(
                    (
                        replace(
                            representative,
                            operation="INSERT",
                            key=current_key,
                            before=None,
                            after=current,
                        ),
                        "c",
                    )
                )
                decision_trace.append(
                    {
                        "table": table.qualified_name,
                        "key": current_key,
                        "source_row": current,
                        "operations": [item.operation for item in replays],
                        "decision": "EMIT",
                        "output_op": "c",
                        "reason": "current source row exists",
                    }
                )
            else:
                # Không còn row: chỉ DELETE mới đủ điều kiện phát op=d.
                if not deletes:
                    decision_trace.append(
                        {
                            "table": table.qualified_name,
                            "key": replays[0].key,
                            "source_row": None,
                            "operations": [item.operation for item in replays],
                            "decision": "BYPASS",
                            "reason": "insert/update key no longer exists in current source",
                        }
                    )
                    continue
                decisions.append((deletes[-1], "d"))
                decision_trace.append(
                    {
                        "table": table.qualified_name,
                        "key": deletes[-1].key,
                        "source_row": None,
                        "operations": [item.operation for item in replays],
                        "decision": "EMIT",
                        "output_op": "d",
                        "reason": "deleted key is absent from current source",
                    }
                )
        return decisions, decision_trace

    @staticmethod
    def _build_kafka_records(
        decisions: list[RepairDecision],
        event: AlertEvent,
        connector_config: ConnectorRuntimeConfig,
        metadata_by_table: dict[TableRef, TableMetadata],
        start_scn: int,
        start_time_ms: int | None,
        commit_time_ms: int | None,
        table_count: int,
    ) -> tuple[list[RepairRecord], dict[str, int]]:
        """Bước 7: chuyển quyết định thành key/value/header kiểu Debezium."""
        stats = {
            "create": 0,
            "update": 0,
            "delete": 0,
            "tables": table_count,
        }
        records: list[RepairRecord] = []
        collection_orders: dict[TableRef, int] = defaultdict(int)
        for replay, op in decisions:
            metadata = metadata_by_table[replay.table]
            stats[{"c": "create", "d": "delete"}[op]] += 1
            topic = ".".join(
                (
                    connector_config.topic_prefix,
                    adjust_topic_component(replay.table.owner),
                    adjust_topic_component(replay.table.name),
                )
            )
            collection_orders[replay.table] += 1
            transaction = None
            if connector_config.provide_transaction_metadata:
                transaction = {
                    "id": event.xid,
                    "total_order": replay.order,
                    "data_collection_order": collection_orders[replay.table],
                }
            processing_ns = time.time_ns()
            records.append(
                RepairRecord(
                    topic=topic,
                    key=_message_key(replay, metadata, connector_config),
                    value={
                        "before": _convert_row(
                            replay.before, metadata, connector_config
                        ),
                        "after": _convert_row(
                            replay.after, metadata, connector_config
                        ),
                        "source": _source(
                            event,
                            replay,
                            connector_config,
                            start_scn,
                            start_time_ms,
                            commit_time_ms,
                        ),
                        "transaction": transaction,
                        "op": op,
                        "ts_ms": processing_ns // 1_000_000,
                        "ts_us": processing_ns // 1_000,
                        "ts_ns": processing_ns,
                    },
                    headers=(
                        (
                            "__debezium.context.connectorName",
                            event.connector_name or connector_config.connector_name,
                        ),
                        (
                            "__debezium.context.connectorLogicalName",
                            event.connector_logical_name or connector_config.topic_prefix,
                        ),
                        (
                            "__debezium.context.taskId",
                            event.task_id or connector_config.task_id,
                        ),
                        (
                            "__debezium.context.runId",
                            event.run_id or connector_config.run_id or "",
                        ),
                    ),
                )
            )
        return records, stats

    @staticmethod
    def _row_identity(row: Row, key_columns: tuple[str, ...]) -> RowIdentity:
        """Khóa nội bộ dùng để tra row theo đúng thứ tự composite PK."""
        return tuple(row[column] for column in key_columns)
