from __future__ import annotations

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

from remediation.config import Settings
from remediation.connect_client import ConnectClient
from remediation.models import AlertEvent
from remediation.oracle_client import OracleClient
from remediation.reconciler import Reconciler


XID_PATTERN = re.compile(r"^[0-9A-Fa-f]{16}$")


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        if name and name not in os.environ:
            os.environ[name] = value.strip().strip("\"'")


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


class TracePrinter:
    def query(self, stage: str, sql: str, binds: Mapping[str, Any]) -> None:
        print(f"\n=== SQL [{stage}] ===")
        print(sql)
        print("--- BINDS ---")
        _print_json(binds)

    def result(self, stage: str, value: Any) -> None:
        print(f"\n=== OUTPUT [{stage}] ===")
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run one abandoned Oracle transaction through the current remediation "
            "code and print every reconstruction/source query. Nothing is published."
        )
    )
    parser.add_argument("transaction_id", help="16-character Oracle XID in hexadecimal")
    parser.add_argument(
        "--connector",
        help="Kafka Connect connector name; auto-selected when exactly one exists",
    )
    args = parser.parse_args()

    xid = args.transaction_id.strip().upper()
    if not XID_PATTERN.fullmatch(xid):
        parser.error("transaction_id must contain exactly 16 hexadecimal characters")

    _load_dotenv(ROOT / ".env")
    settings = Settings.from_env()
    printer = TracePrinter()
    connect = ConnectClient(settings.connect_url)
    oracle: OracleClient | None = None
    try:
        connector = _resolve_connector(connect, args.connector)
        connector_config = connect.get_runtime_config(connector)
        oracle = OracleClient(
            settings.oracle_user,
            settings.oracle_password,
            settings.oracle_dsn,
            1,
            1,
            query_observer=printer.query,
        )
        event = AlertEvent(
            connector=connector,
            xid=xid,
            detected_at=datetime.now(timezone.utc),
            log_line=f"manual dry-run trace for transaction {xid}",
        )

        print("=== INPUT ===")
        _print_json(
            {
                "transaction_id": xid,
                "connector": connector,
                "mode": "dry-run; no Kafka publish and no database writes",
            }
        )
        records, stats = Reconciler(
            oracle,
            trace_observer=printer.result,
        ).build_repairs(event, connector_config)
        print("\n=== DRY-RUN COMPLETE ===")
        _print_json({"repair_message_count": len(records), "stats": stats})
        return 0
    finally:
        if oracle is not None:
            oracle.close()
        connect.close()


if __name__ == "__main__":
    raise SystemExit(main())
