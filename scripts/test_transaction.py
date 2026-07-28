from __future__ import annotations

"""Dry-run một XID theo từng bước production; không publish Kafka, không ghi Oracle."""

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.application.event_reconstructor import reconstruct_events
from src.application.reconciler import Reconciler
from src.configuration.parsers import resolve_topic_bindings
from src.configuration.repository import SqlServerConfigRepository
from src.connect.client import ConnectClient
from src.domain.models import AlertEvent
from src.oracle.client import OracleClient
from src.support.config import Settings


XID_PATTERN = re.compile(r"^[0-9A-Fa-f]{16}$")
QUERY_STEPS = {
    "transaction_summary": (1, "Tìm table và khoảng SCN"),
    "table_columns": (1, "Đọc columns của source table"),
    "primary_keys": (1, "Đọc primary key của source table"),
    "logminer_logfile_discovery": (2, "Tìm redo/archive log chứa khoảng SCN"),
    "logminer_add_logfile": (2, "Đăng ký logfile với LogMiner"),
    "logminer_start": (2, "Khởi động LogMiner"),
    "logminer_dml": (2, "Đọc ordered DML theo XID"),
    "logminer_end": (2, "Đóng LogMiner"),
    "source_as_of": (3, "SELECT full row trước transaction"),
    "source_current": (5, "SELECT trạng thái hiện tại dưới source"),
}


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() and name.strip() not in os.environ:
            os.environ[name.strip()] = value.strip().strip("\"'")


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def _print_json(value: Any) -> None:
    print(json.dumps(_json_safe(value), ensure_ascii=False, indent=2))


class StepPrinter:
    def __init__(self, selected_step: str) -> None:
        self._selected_step = selected_step

    def visible(self, step: int) -> bool:
        return self._selected_step == "all" or self._selected_step == str(step)

    def query(self, stage: str, sql: str, binds: Mapping[str, Any]) -> None:
        section = QUERY_STEPS.get(stage)
        if section is None or not self.visible(section[0]):
            return
        step, title = section
        print(f"\n=== BƯỚC {step} - SQL: {title} ===")
        print(sql.strip())
        print("--- BINDS ---")
        _print_json(binds)

    def output(
        self,
        step: int,
        title: str,
        value: Any,
        code_location: str,
    ) -> None:
        if not self.visible(step):
            return
        print(f"\n=== BƯỚC {step} - OUTPUT: {title} ===")
        print(f"CODE: {code_location}")
        _print_json(value)


def _resolve_connector(connect: ConnectClient, requested: str | None) -> str:
    if requested:
        return requested
    configured = os.getenv("CONNECTOR_NAME", "").strip()
    if configured:
        return configured
    connectors = connect.list_connectors()
    if len(connectors) == 1:
        return connectors[0]
    if not connectors:
        raise RuntimeError("Kafka Connect currently has no connector")
    raise RuntimeError(
        "Kafka Connect has multiple connectors; pass --connector. "
        f"Available: {', '.join(connectors)}"
    )


def _stop_after(selected_step: str, completed_step: int) -> bool:
    return selected_step != "all" and int(selected_step) == completed_step


def main() -> int:
    # Windows PowerShell có thể mặc định CP1252; ép UTF-8 để in comment tiếng Việt.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description=(
            "Dry-run one Oracle transaction and print full SQL/output for one step "
            "or all seven production steps."
        )
    )
    parser.add_argument("transaction_id", help="Oracle XID gồm đúng 16 ký tự hex")
    parser.add_argument(
        "--step",
        choices=("1", "2", "3", "4", "5", "6", "7", "all"),
        default="all",
        help="Step cần chạy và in; mặc định all",
    )
    parser.add_argument(
        "--connector",
        help="Tên Kafka Connect connector; tự chọn nếu hệ thống chỉ có một connector",
    )
    args = parser.parse_args()

    xid = args.transaction_id.strip().upper()
    if not XID_PATTERN.fullmatch(xid):
        parser.error("transaction_id must contain exactly 16 hexadecimal characters")

    _load_dotenv(ROOT / ".env")
    settings = Settings.from_env()
    printer = StepPrinter(args.step)
    connect = ConnectClient(settings.connect_url)
    oracle: OracleClient | None = None
    try:
        connector = _resolve_connector(connect, args.connector)
        config = connect.get_runtime_config(connector)
        guard_config = SqlServerConfigRepository(
            settings.config_db_host,
            settings.config_db_port,
            settings.config_db_name,
            settings.config_db_user,
            settings.config_db_password,
        ).get_by_connector(connector)
        topic_bindings = resolve_topic_bindings(
            guard_config.topics,
            config,
            guard_config.credential.username,
        )
        topic_by_table = {
            binding.table: binding.full_topic for binding in topic_bindings
        }
        credential = guard_config.credential
        oracle_host = credential.host
        if (
            oracle_host.lower() in {"localhost", "127.0.0.1"}
            and settings.oracle_localhost_alias
        ):
            oracle_host = settings.oracle_localhost_alias
        oracle = OracleClient(
            credential.username,
            credential.password,
            f"{oracle_host}:{credential.port}/{credential.database}",
            1,
            1,
            query_observer=printer.query,
        )
        reconciler = Reconciler(oracle)
        event = AlertEvent(
            connector=connector,
            xid=xid,
            detected_at=datetime.now(timezone.utc),
            log_line=f"manual dry-run for transaction {xid}",
        )

        print("=== INPUT ===")
        _print_json(
            {
                "transaction_id": xid,
                "connector": connector,
                "selected_step": args.step,
                "kafka_publish": False,
                "database_write": False,
            }
        )

        # BƯỚC 1: transaction scope, metadata và khoảng SCN.
        included, by_table = reconciler._find_transaction_scope(
            event, config, topic_by_table
        )
        if not by_table:
            printer.output(1, "Không có table thuộc connector", [], "")
            return 0
        metadata_by_table = {
            table: oracle.get_table_metadata(table, config.pdb_name)
            for table in by_table
        }
        start_scn, commit_scn, start_time_ms, commit_time_ms = (
            reconciler._transaction_bounds(included)
        )
        printer.output(
            1,
            "Transaction scope và metadata",
            {
                "summaries": included,
                "metadata": list(metadata_by_table.values()),
                "start_scn": start_scn,
                "commit_scn": commit_scn,
            },
            "src/application/reconciler.py::_find_transaction_scope",
        )
        if _stop_after(args.step, 1):
            return 0

        # BƯỚC 2: LogMiner trả ordered DML đã parse thành delta.
        mined = oracle.mine_transaction(
            xid,
            metadata_by_table,
            start_scn,
            commit_scn,
            config.pdb_name,
            config.include_redo_sql,
        )
        reconciler._validate_mined_count(mined, included)
        printer.output(
            2,
            "Mined changes",
            mined,
            "src/oracle/client.py::mine_transaction",
        )
        if _stop_after(args.step, 2):
            return 0

        # BƯỚC 3: lấy full row trước transaction.
        seed_rows = reconciler._load_seed_rows(
            mined,
            metadata_by_table,
            start_scn,
            config.pdb_name,
        )
        printer.output(
            3,
            "Source rows AS OF START_SCN",
            seed_rows,
            "src/application/reconciler.py::_load_seed_rows",
        )
        if _stop_after(args.step, 3):
            return 0

        def seed_loader(metadata: Any, key: dict[str, Any]) -> dict[str, Any] | None:
            identity = reconciler._row_identity(key, metadata.key_columns)
            return seed_rows.get(metadata.table, {}).get(identity)

        # BƯỚC 4: dựng full before/after theo thứ tự DML.
        replay_events = reconstruct_events(mined, metadata_by_table, seed_loader)
        printer.output(
            4,
            "Reconstructed I/U/D events",
            replay_events,
            "src/application/event_reconstructor.py::reconstruct_events",
        )
        if _stop_after(args.step, 4):
            return 0

        # BƯỚC 5: SELECT trạng thái source hiện tại theo PK.
        grouped = reconciler._group_events_by_primary_key(
            replay_events,
            metadata_by_table,
        )
        current_rows = reconciler._load_current_rows(
            grouped,
            metadata_by_table,
            config.pdb_name,
        )
        printer.output(
            5,
            "Current source rows",
            current_rows,
            "src/application/reconciler.py::_load_current_rows",
        )
        if _stop_after(args.step, 5):
            return 0

        # BƯỚC 6: quyết định emit c, emit d hoặc bypass.
        decisions, decision_trace = reconciler._choose_repairs(
            grouped,
            current_rows,
            metadata_by_table,
        )
        printer.output(
            6,
            "Repair decisions",
            {"decisions": decisions, "details": decision_trace},
            "src/application/reconciler.py::_choose_repairs",
        )
        if _stop_after(args.step, 6):
            return 0

        # BƯỚC 7: dựng Kafka record nhưng không publish.
        records, stats = reconciler._build_kafka_records(
            decisions,
            event,
            config,
            metadata_by_table,
            start_scn,
            start_time_ms,
            commit_time_ms,
            len(by_table),
            topic_by_table,
        )
        printer.output(
            7,
            "Final Kafka messages",
            {"records": records, "stats": stats},
            "src/application/reconciler.py::_build_kafka_records",
        )
        return 0
    finally:
        if oracle is not None:
            oracle.close()
        connect.close()


if __name__ == "__main__":
    raise SystemExit(main())
