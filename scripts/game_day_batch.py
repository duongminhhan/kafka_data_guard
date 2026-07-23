from __future__ import annotations

import argparse
import json
import os
import random
import re
import secrets
import sys
import threading
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import oracledb
except ModuleNotFoundError:  # --dry-run and workload unit tests do not need Oracle
    oracledb = None  # type: ignore[assignment]


IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")


def quoted(name: str) -> str:
    if not IDENTIFIER.fullmatch(name):
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")
    return f'"{name.upper()}"'


@dataclass(frozen=True)
class WorkItem:
    number: int
    key: int
    captured_events: int
    pattern: str
    table: str


@dataclass
class WorkResult:
    number: int
    key: int
    captured_events: int
    pattern: str
    table: str
    local_xid: str | None = None
    start_scn: int | None = None
    outcome: str = "pending"
    error: str | None = None


def build_workload(
    total: int,
    zero_event_count: int,
    owner: str,
    captured_table: str,
    zero_event_table: str,
    seed: int,
    key_base: int,
) -> list[WorkItem]:
    if total < 1:
        raise ValueError("--transactions must be at least 1")
    if zero_event_count < 0 or zero_event_count >= total:
        raise ValueError("--zero-event-transactions must be between 0 and total - 1")

    rng = random.Random(seed)
    zero_numbers = set(rng.sample(range(1, total + 1), zero_event_count))
    work: list[WorkItem] = []
    actionable_number = 0
    for number in range(1, total + 1):
        key = key_base + number
        if number in zero_numbers:
            work.append(
                WorkItem(
                    number,
                    key,
                    0,
                    "excluded-insert-delete",
                    f"{owner}.{zero_event_table}",
                )
            )
            continue

        actionable_number += 1
        captured_events = 2 if actionable_number % 2 else 3
        if captured_events == 2:
            pattern = "insert-update"
        elif actionable_number % 4 == 0:
            pattern = "insert-update-delete"
        else:
            pattern = "insert-update-update"
        work.append(
            WorkItem(
                number,
                key,
                captured_events,
                pattern,
                f"{owner}.{captured_table}",
            )
        )
    return work


def default_key_base(total: int, precision: int = 10) -> int:
    """Return a positive block base that fits NUMBER(precision, 0)."""
    if precision < 2 or total >= 10**precision - 1:
        raise ValueError(f"NUMBER({precision},0) is too small for {total} test keys")
    upper_base = 10**precision - total - 1
    # Reserve the low range and vary each invocation to avoid collisions with old runs.
    lower_base = min(10_000_000, max(1, upper_base // 10))
    if lower_base >= upper_base:
        lower_base = 1
    return lower_base + secrets.randbelow(upper_base - lower_base)


def resolve_key_base(
    dsn: str,
    user: str,
    password: str,
    owner: str,
    tables: tuple[str, ...],
    total: int,
    requested_base: int | None,
) -> int:
    with oracledb.connect(user=user, password=password, dsn=dsn) as connection:
        with connection.cursor() as cursor:
            binds = ", ".join(f":table_{index}" for index in range(len(tables)))
            params: dict[str, Any] = {
                "owner": owner.upper(),
                **{f"table_{index}": table.upper() for index, table in enumerate(tables)},
            }
            cursor.execute(
                f"""
                SELECT DATA_TYPE, DATA_PRECISION, DATA_SCALE
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = :owner
                  AND TABLE_NAME IN ({binds})
                  AND COLUMN_NAME = 'ID'
                """,
                params,
            )
            metadata = list(cursor)
            if len(metadata) != len(set(tables)):
                raise RuntimeError("Could not find numeric ID metadata for both test tables")
            if any(str(row[0]) != "NUMBER" or row[2] not in (None, 0) for row in metadata):
                raise RuntimeError("Batch test requires an integer NUMBER ID column")
            finite_precisions = [int(row[1]) for row in metadata if row[1] is not None]
            precision = min(finite_precisions, default=18)
            maximum = 10**precision - 1
            candidate = requested_base or default_key_base(total, precision)
            if candidate < 1 or candidate + total > maximum:
                raise ValueError(
                    f"Key range {candidate + 1}..{candidate + total} does not fit "
                    f"NUMBER({precision},0)"
                )

            for _ in range(100):
                collisions = 0
                for table_name in tables:
                    table = f"{quoted(owner)}.{quoted(table_name)}"
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE ID BETWEEN :low AND :high",
                        low=candidate + 1,
                        high=candidate + total,
                    )
                    collisions += int(cursor.fetchone()[0])
                if collisions == 0:
                    return candidate
                candidate += total + 1
                if candidate + total > maximum:
                    candidate = 1
            raise RuntimeError("Could not find a free ID range after 100 attempts")


def _matches_table(patterns: str, qualified_table: str) -> bool:
    values = [value.strip() for value in patterns.split(",") if value.strip()]
    return not values or any(re.fullmatch(value, qualified_table) for value in values)


def check_connector_scope(
    connect_url: str,
    connector: str,
    captured_qualified: str,
    zero_qualified: str,
) -> None:
    url = f"{connect_url.rstrip('/')}/connectors/{connector}/config"
    with urllib.request.urlopen(url, timeout=10) as response:
        config = json.load(response)
    includes = str(config.get("table.include.list") or "")
    excludes = str(config.get("table.exclude.list") or "")
    captured = _matches_table(includes, captured_qualified) and not (
        excludes and _matches_table(excludes, captured_qualified)
    )
    zero = _matches_table(includes, zero_qualified) and not (
        excludes and _matches_table(excludes, zero_qualified)
    )
    if not captured:
        raise RuntimeError(
            f"Captured table {captured_qualified} is not included by connector {connector}"
        )
    if zero:
        raise RuntimeError(
            f"Zero-event table {zero_qualified} is included by connector {connector}; "
            "it would not produce a 0-event abandonment"
        )


def execute_dml(cursor: oracledb.Cursor, item: WorkItem, run_id: str) -> None:
    owner, table_name = item.table.split(".", 1)
    table = f"{quoted(owner)}.{quoted(table_name)}"
    payload = f"batch-{run_id}-tx-{item.number}"
    cursor.execute(
        f"INSERT INTO {table} (ID, PAYLOAD, CREATED_AT) "
        "VALUES (:id, :payload, SYSTIMESTAMP)",
        id=item.key,
        payload=f"{payload}-insert",
    )
    cursor.execute(
        f"UPDATE {table} SET PAYLOAD = :payload WHERE ID = :id",
        id=item.key,
        payload=f"{payload}-update-1",
    )
    if item.pattern == "insert-update-update":
        cursor.execute(
            f"UPDATE {table} SET PAYLOAD = :payload WHERE ID = :id",
            id=item.key,
            payload=f"{payload}-update-2",
        )
    elif item.pattern in {"insert-update-delete", "excluded-insert-delete"}:
        cursor.execute(f"DELETE FROM {table} WHERE ID = :id", id=item.key)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Open concurrent Oracle transactions to exercise Debezium abandoned-"
            "transaction remediation"
        )
    )
    parser.add_argument("--transactions", type=int, default=20)
    parser.add_argument("--zero-event-transactions", type=int, default=3)
    parser.add_argument("--owner", default="C##CDCUSER")
    parser.add_argument("--captured-table", default="CDC_REMEDIATION_POC")
    parser.add_argument("--zero-event-table", default="CDC_ZERO_EVENT_TEST")
    parser.add_argument("--connector", default="oracle-remediation-poc")
    parser.add_argument("--hold-seconds", type=int, default=90)
    parser.add_argument("--seed", type=int, default=20260721)
    parser.add_argument("--key-base", type=int)
    parser.add_argument("--commit", action="store_true", help="Commit after the hold; default rolls back")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--dsn", default=os.getenv("TEST_ORACLE_DSN") or os.getenv("ORACLE_DSN"))
    parser.add_argument("--user", default=os.getenv("TEST_ORACLE_USER") or os.getenv("ORACLE_USER"))
    args = parser.parse_args()

    password = os.getenv("TEST_ORACLE_PASSWORD") or os.getenv("ORACLE_PASSWORD")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.dry_run:
        work = build_workload(
            args.transactions,
            args.zero_event_transactions,
            args.owner.upper(),
            args.captured_table.upper(),
            args.zero_event_table.upper(),
            args.seed,
            args.key_base or default_key_base(args.transactions),
        )
        print(
            json.dumps(
                {"run_id": run_id, "commit": args.commit, "workload": [asdict(x) for x in work]},
                indent=2,
            )
        )
        return
    if oracledb is None:
        raise RuntimeError(
            "Python package 'oracledb' is missing; install requirements-dev.txt"
        )
    if not args.dsn or not args.user or not password:
        raise RuntimeError(
            "Set TEST_ORACLE_DSN, TEST_ORACLE_USER and TEST_ORACLE_PASSWORD "
            "(or the corresponding ORACLE_* variables)"
        )
    if args.hold_seconds < 1:
        raise ValueError("--hold-seconds must be positive")

    captured_qualified = f"{args.owner.upper()}.{args.captured_table.upper()}"
    zero_qualified = f"{args.owner.upper()}.{args.zero_event_table.upper()}"
    connect_url = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
    check_connector_scope(
        connect_url, args.connector, captured_qualified, zero_qualified
    )
    key_base = resolve_key_base(
        args.dsn,
        args.user,
        password,
        args.owner,
        (args.captured_table, args.zero_event_table),
        args.transactions,
        args.key_base,
    )
    work = build_workload(
        args.transactions,
        args.zero_event_transactions,
        args.owner.upper(),
        args.captured_table.upper(),
        args.zero_event_table.upper(),
        args.seed,
        key_base,
    )
    print(
        json.dumps(
            {"run_id": run_id, "commit": args.commit, "workload": [asdict(x) for x in work]},
            indent=2,
        )
    )

    condition = threading.Condition()
    release = threading.Event()
    results: list[WorkResult] = []
    ready = 0
    finished = 0

    def worker(item: WorkItem) -> None:
        nonlocal ready, finished
        result = WorkResult(**asdict(item))
        connection: oracledb.Connection | None = None
        try:
            connection = oracledb.connect(
                user=args.user, password=password, dsn=args.dsn
            )
            with connection.cursor() as cursor:
                execute_dml(cursor, item, run_id)
                cursor.execute("SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM DUAL")
                result.local_xid = str(cursor.fetchone()[0])
            result.outcome = "holding"
            with condition:
                results.append(result)
                ready += 1
                print(
                    f"READY {ready:02d}/{len(work)} tx={item.number:02d} "
                    f"events={item.captured_events} pattern={item.pattern} "
                    f"local_xid={result.local_xid}",
                    flush=True,
                )
                condition.notify_all()
            release.wait()
            if args.commit:
                connection.commit()
                result.outcome = "committed"
            else:
                connection.rollback()
                result.outcome = "rolled_back"
        except Exception as exc:  # each session reports its own Oracle failure
            if connection is not None:
                connection.rollback()
            result.outcome = "failed"
            result.error = f"{type(exc).__name__}: {exc}"
            with condition:
                if result not in results:
                    results.append(result)
                condition.notify_all()
        finally:
            if connection is not None:
                connection.close()
            with condition:
                finished += 1
                condition.notify_all()

    threads = [
        threading.Thread(target=worker, args=(item,), name=f"oracle-tx-{item.number}")
        for item in work
    ]
    for thread in threads:
        thread.start()

    startup_deadline = time.monotonic() + 60
    with condition:
        while ready < len(work) and finished == 0 and time.monotonic() < startup_deadline:
            condition.wait(timeout=1)
    failures = [result for result in results if result.outcome == "failed"]
    if ready != len(work) or failures:
        release.set()
        for thread in threads:
            thread.join()
        details = "; ".join(result.error or "unknown error" for result in failures)
        raise RuntimeError(
            f"Only {ready}/{len(work)} transactions became ready. {details}"
        )

    print(
        f"All {ready} transactions are open. Holding for {args.hold_seconds}s; "
        f"connector retention must be lower than this value.",
        flush=True,
    )
    time.sleep(args.hold_seconds)
    release.set()
    for thread in threads:
        thread.join()

    ordered_results = sorted(results, key=lambda item: item.number)
    summary: dict[str, Any] = {
        "run_id": run_id,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "transactions": len(ordered_results),
        "actionable_transactions": sum(x.captured_events > 0 for x in ordered_results),
        "zero_event_transactions": sum(x.captured_events == 0 for x in ordered_results),
        "expected_captured_events": sum(x.captured_events for x in ordered_results),
        "outcome": "committed" if args.commit else "rolled_back",
        "results": [asdict(x) for x in ordered_results],
    }
    print(json.dumps(summary, indent=2))
    if args.manifest:
        args.manifest.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(f"Manifest written to {args.manifest}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
