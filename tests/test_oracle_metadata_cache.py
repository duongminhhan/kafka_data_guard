from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import sleep
from unittest.mock import patch

import remediation.oracle_client as oracle_client_module
from remediation.models import TableMetadata, TableRef
from remediation.oracle_client import OracleClient


TABLE = TableRef("CDCUSER", "ORDERS")
METADATA = TableMetadata(
    table=TABLE,
    columns=("ID", "PAYLOAD"),
    key_columns=("ID",),
    column_types={"ID": "NUMBER", "PAYLOAD": "VARCHAR2"},
    column_scales={"ID": 0, "PAYLOAD": None},
)


def _client() -> OracleClient:
    with patch.object(oracle_client_module.oracledb, "create_pool"):
        return OracleClient("user", "password", "dsn", 1, 2)


def test_table_metadata_cache_is_keyed_case_insensitively_by_pdb_owner_table() -> None:
    client = _client()
    loads: list[tuple[TableRef, str | None]] = []

    def load(table: TableRef, pdb_name: str | None = None) -> TableMetadata:
        loads.append((table, pdb_name))
        return METADATA

    client._load_table_metadata = load  # type: ignore[method-assign]

    assert client.get_table_metadata(TABLE, "ORCLPDB1") is METADATA
    assert (
        client.get_table_metadata(TableRef("cdcuser", "orders"), "orclpdb1")
        is METADATA
    )
    assert loads == [(TABLE, "ORCLPDB1")]


def test_table_metadata_cache_reloads_after_ttl() -> None:
    client = _client()
    current_time = 100.0
    load_count = 0

    def clock() -> float:
        return current_time

    def load(_table: TableRef, _pdb_name: str | None = None) -> TableMetadata:
        nonlocal load_count
        load_count += 1
        return METADATA

    client._load_table_metadata = load  # type: ignore[method-assign]
    with patch.object(oracle_client_module, "monotonic", clock):
        client.get_table_metadata(TABLE, "ORCLPDB1")
        current_time += client.TABLE_METADATA_CACHE_TTL_SECONDS - 1
        client.get_table_metadata(TABLE, "ORCLPDB1")
        current_time += 2
        client.get_table_metadata(TABLE, "ORCLPDB1")

    assert load_count == 2


def test_concurrent_cache_misses_for_same_table_are_collapsed() -> None:
    client = _client()
    load_count = 0
    count_lock = Lock()

    def load(_table: TableRef, _pdb_name: str | None = None) -> TableMetadata:
        nonlocal load_count
        with count_lock:
            load_count += 1
        sleep(0.02)
        return METADATA

    client._load_table_metadata = load  # type: ignore[method-assign]
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(
                lambda _: client.get_table_metadata(TABLE, "ORCLPDB1"),
                range(16),
            )
        )

    assert results == [METADATA] * 16
    assert load_count == 1
