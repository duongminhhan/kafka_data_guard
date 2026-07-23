from __future__ import annotations

import argparse
import os
import re
import time

import oracledb


IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")


def quoted(name: str) -> str:
    if not IDENTIFIER.fullmatch(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f'"{name}"'


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hold one UAT UPDATE transaction open long enough to trigger abandonment"
    )
    parser.add_argument("--owner", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--key-column", required=True)
    parser.add_argument("--key-value", required=True)
    parser.add_argument("--value-column", required=True)
    parser.add_argument("--value", default="cdc-remediation-game-day")
    parser.add_argument("--hold-seconds", type=int, default=300)
    parser.add_argument("--commit", action="store_true", help="Commit; default is rollback")
    args = parser.parse_args()

    dsn = os.environ["ORACLE_DSN"]
    user = os.environ["ORACLE_USER"]
    password = os.environ["ORACLE_PASSWORD"]
    table = f"{quoted(args.owner)}.{quoted(args.table)}"
    sql = (
        f"UPDATE {table} SET {quoted(args.value_column)} = :value "
        f"WHERE {quoted(args.key_column)} = :key_value"
    )

    with oracledb.connect(user=user, password=password, dsn=dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, value=args.value, key_value=args.key_value)
            if cursor.rowcount != 1:
                connection.rollback()
                raise RuntimeError(f"Expected one UAT row, updated {cursor.rowcount}")
            cursor.execute("SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM DUAL")
            local_xid = cursor.fetchone()[0]
            print(f"Local Oracle transaction id: {local_xid}", flush=True)
            print(f"Holding transaction for {args.hold_seconds}s...", flush=True)
            time.sleep(args.hold_seconds)
            if args.commit:
                connection.commit()
                print("Transaction committed", flush=True)
            else:
                connection.rollback()
                print("Transaction rolled back (use --commit for end-to-end repair test)")


if __name__ == "__main__":
    main()

