from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Mapping

from remediation.domain.models import TableMetadata, TableRef
from remediation.oracle.client import OracleClient


class _Cursor:
    description: list[Any] = []

    def __init__(self) -> None:
        self._container_query = False

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, sql: str, *_args: Any, **_kwargs: Any) -> None:
        self._container_query = "SYS_CONTEXT('USERENV', 'CON_NAME')" in sql

    def fetchone(self) -> tuple[str]:
        assert self._container_query
        return ("CDB$ROOT",)

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(())


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor


def _client(
    events: list[tuple[str, str, Mapping[str, Any]]],
) -> OracleClient:
    client = object.__new__(OracleClient)
    client._query_observer = lambda stage, sql, binds: events.append(
        (stage, sql, binds)
    )
    cursor = _Cursor()

    @contextmanager
    def connection() -> Iterator[_Connection]:
        yield _Connection(cursor)

    client.connection = connection  # type: ignore[method-assign]
    return client


def _metadata() -> TableMetadata:
    return TableMetadata(
        table=TableRef("C##CDCUSER", "ITEM_COMMENTS"),
        columns=("ITEM_KEY", "COMMENT_SEQ", "CRT_TS"),
        key_columns=("ITEM_KEY", "COMMENT_SEQ"),
        column_types={
            "ITEM_KEY": "NUMBER",
            "COMMENT_SEQ": "NUMBER",
            "CRT_TS": "TIMESTAMP",
        },
        column_scales={"ITEM_KEY": 0, "COMMENT_SEQ": 0, "CRT_TS": None},
    )


def test_current_source_query_trace_contains_composite_keys_and_binds() -> None:
    events: list[tuple[str, str, Mapping[str, Any]]] = []

    result = _client(events).get_current_rows(
        _metadata(),
        [
            {"ITEM_KEY": "44576735", "COMMENT_SEQ": "1"},
            {"ITEM_KEY": "44576736", "COMMENT_SEQ": "2"},
        ],
    )

    assert result == {}
    stage, sql, binds = events[0]
    assert stage == "source_current"
    assert 'FROM "C##CDCUSER"."ITEM_COMMENTS"' in sql
    assert "AS OF SCN" not in sql
    assert '"ITEM_KEY" = :key_0_0' in sql
    assert '"COMMENT_SEQ" = :key_1_1' in sql
    assert binds == {
        "key_0_0": "44576735",
        "key_0_1": "1",
        "key_1_0": "44576736",
        "key_1_1": "2",
    }


def test_source_as_of_query_trace_contains_start_scn() -> None:
    events: list[tuple[str, str, Mapping[str, Any]]] = []

    _client(events).get_rows_as_of(
        _metadata(),
        [{"ITEM_KEY": "44576735", "COMMENT_SEQ": "1"}],
        6565787,
    )

    stage, sql, binds = events[0]
    assert stage == "source_as_of"
    assert 'FROM "C##CDCUSER"."ITEM_COMMENTS" AS OF SCN :as_of_scn' in sql
    assert binds["as_of_scn"] == 6565787
