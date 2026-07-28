from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator, Mapping

import pytest

from src.oracle.logminer_parser import (
    LogMinerSqlParseError,
    parse_logminer_change,
)
from src.domain.models import TableMetadata, TableRef
from src.oracle.client import OracleClient


TABLE = TableRef("C##CDCUSER", "CDC_REMEDIATION_POC")
METADATA = TableMetadata(
    table=TABLE,
    columns=("ID", "PAYLOAD", "CREATED_AT"),
    key_columns=("ID",),
    column_types={
        "ID": "NUMBER",
        "PAYLOAD": "VARCHAR2",
        "CREATED_AT": "TIMESTAMP",
    },
    column_scales={"ID": 0, "PAYLOAD": None, "CREATED_AT": None},
)


def test_parses_insert_with_quoted_comma_and_timestamp() -> None:
    _, after = parse_logminer_change(
        "INSERT",
        """
        insert into "C##CDCUSER"."CDC_REMEDIATION_POC"
          ("ID","PAYLOAD","CREATED_AT")
        values (7241001,'O''Brien, event',
                TO_TIMESTAMP('2026-07-23 15:30:01.123456','YYYY-MM-DD HH24:MI:SS.FF'));
        """,
        "delete from \"C##CDCUSER\".\"CDC_REMEDIATION_POC\" where \"ID\" = 7241001;",
        METADATA,
    )

    assert after == {
        "ID": "7241001",
        "PAYLOAD": "O'Brien, event",
        "CREATED_AT": datetime(2026, 7, 23, 15, 30, 1, 123456),
    }


def test_parses_update_before_and_after_from_redo_and_undo() -> None:
    before, after = parse_logminer_change(
        "UPDATE",
        """
        update "C##CDCUSER"."CDC_REMEDIATION_POC"
        set "PAYLOAD" = 'new AND final'
        where "ID" = 7241002 and "PAYLOAD" = 'old';
        """,
        """
        update "C##CDCUSER"."CDC_REMEDIATION_POC"
        set "PAYLOAD" = 'old'
        where "ID" = 7241002 and "PAYLOAD" = 'new AND final';
        """,
        METADATA,
    )

    assert before == {"ID": "7241002", "PAYLOAD": "old"}
    assert after == {"ID": "7241002", "PAYLOAD": "new AND final"}


def test_delete_prefers_full_row_from_sql_undo_insert() -> None:
    before, after = parse_logminer_change(
        "DELETE",
        """
        delete from "C##CDCUSER"."CDC_REMEDIATION_POC"
        where "ID" = 7241011 and "PAYLOAD" = 'deleted';
        """,
        """
        insert into "C##CDCUSER"."CDC_REMEDIATION_POC"
          ("ID","PAYLOAD","CREATED_AT")
        values (7241011,'deleted',NULL);
        """,
        METADATA,
    )

    assert before == {
        "ID": "7241011",
        "PAYLOAD": "deleted",
        "CREATED_AT": None,
    }
    assert after == {}


def test_parser_fails_closed_for_unsupported_expression() -> None:
    with pytest.raises(LogMinerSqlParseError, match="Unsupported Oracle value"):
        parse_logminer_change(
            "UPDATE",
            """
            update "C##CDCUSER"."CDC_REMEDIATION_POC"
            set "PAYLOAD" = SOME_UNKNOWN_FUNCTION('value')
            where "ID" = 7241002;
            """,
            None,
            METADATA,
        )


class _LogMinerCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows

    def execute(self, _sql: str, **_binds: Any) -> None:
        return None

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self.rows)


def test_mine_sql_statements_joins_csf_fragments_before_parsing() -> None:
    now = datetime(2026, 7, 23, 15, 30)
    common = (
        6565787,
        "0x000001.00000001.0001",
        0,
    )
    rows = [
        common
        + (
            1,
            77,
            "04001500F8030000",
            TABLE.owner,
            TABLE.name,
            "AAAROWID",
            "AAAAAAAAHAAAAGuAAH",
            "UPDATE",
            'update "C##CDCUSER"."CDC_REMEDIATION_POC" set "PAYLOAD" = ',
            'update "C##CDCUSER"."CDC_REMEDIATION_POC" set "PAYLOAD" = ',
            6565797,
            now,
            now,
            now,
            1,
            TABLE.owner,
        ),
        common
        + (
            0,
            77,
            "04001500F8030000",
            TABLE.owner,
            TABLE.name,
            "AAAROWID",
            "AAAAAAAAHAAAAGuAAH",
            "UPDATE",
            "'new' where \"ID\" = 7241002;",
            "'old' where \"ID\" = 7241002;",
            6565797,
            now,
            now,
            now,
            1,
            TABLE.owner,
        ),
    ]
    query_events: list[tuple[str, str, Mapping[str, Any]]] = []
    client = object.__new__(OracleClient)
    client._query_observer = lambda stage, sql, binds: query_events.append(
        (stage, sql, binds)
    )

    changes = client._mine_sql_statements(
        _LogMinerCursor(rows),  # type: ignore[arg-type]
        "04001500F8030000",
        {TABLE: METADATA},
    )

    assert len(changes) == 1
    assert changes[0].before_delta == {"ID": "7241002", "PAYLOAD": "old"}
    assert changes[0].after_delta == {"ID": "7241002", "PAYLOAD": "new"}
    assert changes[0].redo_sql.endswith("'new' where \"ID\" = 7241002;")
    assert changes[0].undo_sql.endswith("'old' where \"ID\" = 7241002;")
    assert query_events[0][0] == "logminer_dml"
    assert "CSF" in query_events[0][1]
    assert "SQL_UNDO" in query_events[0][1]
