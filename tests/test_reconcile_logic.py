from __future__ import annotations

import pytest
from dataclasses import replace
from datetime import datetime, timezone

from src.connect.client import ConnectorRuntimeConfig
from src.domain.models import AlertEvent, MinedChange, TableMetadata, TableRef, TransactionTable
from src.application.event_reconstructor import IncompleteRedoError, reconstruct_events
from src.application.reconciler import IncompleteTransactionError, Reconciler, adjust_topic_component


TABLE = TableRef("C##CDCUSER", "CDC_REMEDIATION_POC")
TABLE_2 = TableRef("SALES", "ORDERS")
METADATA = TableMetadata(
    table=TABLE,
    columns=("ID", "PAYLOAD"),
    key_columns=("ID",),
    column_types={"ID": "NUMBER", "PAYLOAD": "VARCHAR2"},
    column_scales={"ID": 0, "PAYLOAD": None},
)
METADATA_2 = TableMetadata(
    table=TABLE_2,
    columns=("ORDER_ID", "AMOUNT", "STATUS"),
    key_columns=("ORDER_ID",),
    column_types={"ORDER_ID": "NUMBER", "AMOUNT": "NUMBER", "STATUS": "VARCHAR2"},
    column_scales={"ORDER_ID": 0, "AMOUNT": 2, "STATUS": None},
)

COMPOSITE_KEY_METADATA = TableMetadata(
    table=TableRef("TOPOVN", "ITEM_COMMENTS"),
    columns=("ITEM_KEY", "COMMENT_SEQ", "CRT_TS", "COMMENTS"),
    key_columns=("ITEM_KEY", "COMMENT_SEQ", "CRT_TS"),
    column_types={
        "ITEM_KEY": "NUMBER",
        "COMMENT_SEQ": "NUMBER",
        "CRT_TS": "VARCHAR2",
        "COMMENTS": "VARCHAR2",
    },
    column_scales={
        "ITEM_KEY": 0,
        "COMMENT_SEQ": 0,
        "CRT_TS": None,
        "COMMENTS": None,
    },
    column_nullable={
        "ITEM_KEY": False,
        "COMMENT_SEQ": False,
        "CRT_TS": False,
        "COMMENTS": True,
    },
    column_defaults={
        "ITEM_KEY": "0",
        "COMMENT_SEQ": "0",
        "CRT_TS": "'1970-01-01 00:00:00'",
        "COMMENTS": None,
    },
)


def change(
    operation: str,
    order: int,
    row_id: str,
    before: dict | None = None,
    after: dict | None = None,
) -> MinedChange:
    return MinedChange(
        table=TABLE,
        operation=operation,
        scn=100,
        commit_scn=101,
        rs_id=f"0x000001.00000001.{order:04x}",
        ssn=0,
        row_id=row_id,
        redo_thread=1,
        user_name="C##CDCUSER",
        change_time=None,
        start_time=None,
        commit_time=None,
        redo_sql=None,
        before_delta=before or {},
        after_delta=after or {},
    )


def test_adjust_topic_component_matches_common_oracle_schema_topic() -> None:
    assert adjust_topic_component("C##CDCUSER") == "C__CDCUSER"
    assert adjust_topic_component("CDC_REMEDIATION_POC") == "CDC_REMEDIATION_POC"


def test_reconstructs_every_dml_in_original_order() -> None:
    changes = [
        change("INSERT", 1, "RID1", after={"ID": 1, "PAYLOAD": "one"}),
        change(
            "UPDATE",
            2,
            "RID1",
            before={"ID": 1, "PAYLOAD": "one"},
            after={"ID": 1, "PAYLOAD": "two"},
        ),
        change("INSERT", 3, "RID2", after={"ID": 2, "PAYLOAD": "temp"}),
        change("DELETE", 4, "RID2", before={"ID": 2, "PAYLOAD": "temp"}),
        change(
            "UPDATE",
            5,
            "RID1",
            before={"ID": 1, "PAYLOAD": "two"},
            after={"ID": 1, "PAYLOAD": "final"},
        ),
    ]

    events = reconstruct_events(changes, {TABLE: METADATA}, lambda _m, _k: None)

    assert [event.operation for event in events] == [
        "INSERT",
        "UPDATE",
        "INSERT",
        "DELETE",
        "UPDATE",
    ]
    assert events[0].before is None
    assert events[0].after == {"ID": 1, "PAYLOAD": "one"}
    assert events[1].before == {"ID": 1, "PAYLOAD": "one"}
    assert events[1].after == {"ID": 1, "PAYLOAD": "two"}
    assert events[3].before == {"ID": 2, "PAYLOAD": "temp"}
    assert events[3].after is None
    assert events[4].before == {"ID": 1, "PAYLOAD": "two"}
    assert events[4].after == {"ID": 1, "PAYLOAD": "final"}


def test_update_uses_flashback_seed_for_unchanged_columns() -> None:
    changes = [
        change(
            "UPDATE",
            1,
            "RID1",
            before={"ID": 1, "PAYLOAD": "old"},
            after={"ID": 1, "PAYLOAD": "new"},
        )
    ]
    events = reconstruct_events(
        changes,
        {TABLE: METADATA},
        lambda _metadata, _key: {"ID": 1, "PAYLOAD": "old"},
    )
    assert events[0].before == {"ID": 1, "PAYLOAD": "old"}
    assert events[0].after == {"ID": 1, "PAYLOAD": "new"}


def test_primary_key_update_fails_closed() -> None:
    changes = [
        change(
            "UPDATE",
            1,
            "RID1",
            before={"ID": 1},
            after={"ID": 2},
        )
    ]
    with pytest.raises(IncompleteRedoError, match="Primary-key UPDATE"):
        reconstruct_events(
            changes,
            {TABLE: METADATA},
            lambda _metadata, _key: {"ID": 1, "PAYLOAD": "value"},
        )


def test_reconstruction_is_driven_by_metadata_for_other_tables() -> None:
    other_change = replace(
        change("INSERT", 1, "RID-ORDER"),
        table=TABLE_2,
        after_delta={"ORDER_ID": "9001", "AMOUNT": "125.50", "STATUS": "NEW"},
    )

    event = reconstruct_events(
        [other_change], {TABLE_2: METADATA_2}, lambda _metadata, _key: None
    )[0]

    assert event.table == TABLE_2
    assert event.key == {"ORDER_ID": "9001"}
    assert event.after == {
        "ORDER_ID": "9001",
        "AMOUNT": "125.50",
        "STATUS": "NEW",
    }


def test_missing_pre_transaction_row_fails_closed() -> None:
    changes = [
        change("DELETE", 1, "RID1", before={"ID": 1}),
    ]
    with pytest.raises(IncompleteRedoError, match="deleted row"):
        reconstruct_events(changes, {TABLE: METADATA}, lambda _m, _k: None)


class FakeOracle:
    def __init__(
        self,
        changes: list[MinedChange],
        current_rows: dict[str, dict] | None = None,
    ) -> None:
        self.changes = changes
        self.current_rows = current_rows or {}
        self.batch_as_of_calls = 0
        self.batch_current_calls = 0

    def find_transaction_tables(self, _xid: str, _pdb_name=None) -> list[TransactionTable]:
        counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
        for item in self.changes:
            counts[item.operation] += 1
        return [
            TransactionTable(TABLE, count, 100, 101, None, None)
            for operation, count in counts.items()
            if count
        ]

    def get_table_metadata(self, _table: TableRef, _pdb_name=None) -> TableMetadata:
        return METADATA

    def mine_transaction(self, *_args) -> list[MinedChange]:
        return self.changes

    def get_row_as_of(self, *_args):
        return None

    def get_rows_as_of(self, _metadata, keys, *_args):
        self.batch_as_of_calls += 1
        return {}

    def get_current_row(self, _metadata, key, *_args):
        return self.current_rows.get(str(key["ID"]))

    def get_current_rows(self, metadata, keys, *_args):
        self.batch_current_calls += 1
        return {
            tuple(key[column] for column in metadata.key_columns): row
            for key in keys
            if (row := self.current_rows.get(str(key["ID"]))) is not None
        }


def test_reconciler_converges_each_key_to_current_source_state() -> None:
    changes = [
        change("INSERT", 1, "RID1", after={"ID": "1", "PAYLOAD": "one"}),
        change(
            "UPDATE",
            2,
            "RID1",
            before={"ID": "1"},
            after={"ID": "1", "PAYLOAD": "two"},
        ),
        change("DELETE", 3, "RID2", before={"ID": "2", "PAYLOAD": "old-two"}),
        change("DELETE", 4, "RID3", before={"ID": "3", "PAYLOAD": "old-three"}),
    ]
    event = AlertEvent("oracle", "01001000E6030000", datetime.now(timezone.utc), "log")
    config = ConnectorRuntimeConfig(
        "server",
        (),
        (),
        "ORCLCDB",
        connector_version="3.5.2.Final",
        run_id="test-run-id",
    )

    oracle = FakeOracle(
        changes,
        current_rows={
            "1": {"ID": "1", "PAYLOAD": "source-current-one"},
            "2": {"ID": "2", "PAYLOAD": "business-recreated-two"},
        },
    )
    records, stats = Reconciler(oracle).build_repairs(event, config)  # type: ignore[arg-type]

    assert [record.value["op"] for record in records if record.value] == ["c", "d"]
    assert stats == {"create": 1, "update": 0, "delete": 1, "tables": 1}
    assert oracle.batch_as_of_calls == 1
    assert oracle.batch_current_calls == 1

    create = records[0]
    assert create.key == {"ID": "1"}
    assert create.value is not None
    assert create.value["before"] is None
    assert create.value["after"] == {
        "ID": "1",
        "PAYLOAD": "source-current-one",
    }
    assert create.value["source"]["version"] == "3.5.2.Final"
    assert create.value["source"]["snapshot"] == "false"
    assert create.value["source"]["sequence"] is None
    assert create.value["source"]["lcr_position"] is None
    assert create.value["source"]["redo_sql"] is None
    assert create.value["source"]["txSeq"] == 2
    assert "remediation" not in create.value["source"]
    assert isinstance(create.value["ts_ms"], int)
    assert isinstance(create.value["ts_us"], int)
    assert isinstance(create.value["ts_ns"], int)
    assert dict(create.headers) == {
        "__debezium.context.connectorName": "oracle",
        "__debezium.context.connectorLogicalName": "server",
        "__debezium.context.taskId": "0",
        "__debezium.context.runId": "test-run-id",
    }


def test_update_only_history_keeps_debezium_update_operation() -> None:
    replay = reconstruct_events(
        [
            change(
                "UPDATE",
                1,
                "RID1",
                before={"ID": "1", "PAYLOAD": "old"},
                after={"ID": "1", "PAYLOAD": "mined-new"},
            )
        ],
        {TABLE: METADATA},
        lambda _metadata, _key: {"ID": "1", "PAYLOAD": "old"},
    )[0]
    current = {"ID": "1", "PAYLOAD": "source-current"}

    decisions, trace = Reconciler._choose_repairs(
        {(TABLE, ("1",)): [replay]},
        {TABLE: {("1",): current}},
        {TABLE: METADATA},
    )

    repaired, output_op = decisions[0]
    assert output_op == "u"
    assert repaired.operation == "UPDATE"
    assert repaired.before == {"ID": "1", "PAYLOAD": "old"}
    assert repaired.after == current
    assert trace[0]["output_op"] == "u"

    records, stats = Reconciler._build_kafka_records(
        decisions,
        AlertEvent(
            "oracle",
            "01001000E6030000",
            datetime.now(timezone.utc),
            "log",
        ),
        ConnectorRuntimeConfig(
            "server",
            (),
            (),
            connector_version="3.5.2.Final",
        ),
        {TABLE: METADATA},
        100,
        None,
        None,
        1,
    )
    assert records[0].value is not None
    assert records[0].value["op"] == "u"
    assert records[0].value["before"] == {"ID": "1", "PAYLOAD": "old"}
    assert records[0].value["after"] == current
    assert stats == {"create": 0, "update": 1, "delete": 0, "tables": 1}


def test_update_then_delete_emits_only_update_when_key_still_exists() -> None:
    replays = reconstruct_events(
        [
            change(
                "UPDATE",
                1,
                "RID1",
                before={"ID": "1", "PAYLOAD": "old"},
                after={"ID": "1", "PAYLOAD": "updated"},
            ),
            change(
                "DELETE",
                2,
                "RID1",
                before={"ID": "1", "PAYLOAD": "updated"},
            ),
        ],
        {TABLE: METADATA},
        lambda _metadata, _key: {"ID": "1", "PAYLOAD": "old"},
    )
    current = {"ID": "1", "PAYLOAD": "business-recreated"}

    decisions, trace = Reconciler._choose_repairs(
        {(TABLE, ("1",)): replays},
        {TABLE: {("1",): current}},
        {TABLE: METADATA},
    )

    assert [(replay.operation, op) for replay, op in decisions] == [
        ("UPDATE", "u")
    ]
    assert decisions[0][0].after == current
    assert trace[0]["operations"] == ["UPDATE", "DELETE"]
    assert trace[0]["output_op"] == "u"


def test_update_then_delete_emits_both_in_order_when_key_is_absent() -> None:
    replays = reconstruct_events(
        [
            change(
                "UPDATE",
                1,
                "RID1",
                before={"ID": "1", "PAYLOAD": "old"},
                after={"ID": "1", "PAYLOAD": "updated"},
            ),
            change(
                "DELETE",
                2,
                "RID1",
                before={"ID": "1", "PAYLOAD": "updated"},
            ),
        ],
        {TABLE: METADATA},
        lambda _metadata, _key: {"ID": "1", "PAYLOAD": "old"},
    )

    decisions, trace = Reconciler._choose_repairs(
        {(TABLE, ("1",)): replays},
        {TABLE: {}},
        {TABLE: METADATA},
    )
    records, stats = Reconciler._build_kafka_records(
        decisions,
        AlertEvent(
            "oracle",
            "01001000E6030000",
            datetime.now(timezone.utc),
            "log",
        ),
        ConnectorRuntimeConfig(
            "server",
            (),
            (),
            connector_version="3.5.2.Final",
        ),
        {TABLE: METADATA},
        100,
        None,
        None,
        1,
    )

    assert [(replay.order, op) for replay, op in decisions] == [(1, "u"), (2, "d")]
    assert [record.value["op"] for record in records] == ["u", "d"]
    assert records[0].value["before"] == {"ID": "1", "PAYLOAD": "old"}
    assert records[0].value["after"] == {"ID": "1", "PAYLOAD": "updated"}
    assert records[1].value["before"] == {"ID": "1", "PAYLOAD": "updated"}
    assert records[1].value["after"] is None
    assert trace[0]["output_ops"] == ["u", "d"]
    assert stats == {"create": 0, "update": 1, "delete": 1, "tables": 1}


def test_reconciler_builds_schema_enabled_composite_primary_key() -> None:
    table = COMPOSITE_KEY_METADATA.table
    mined = replace(
        change("INSERT", 1, "RID-COMMENT"),
        table=table,
        after_delta={
            "ITEM_KEY": "44576735",
            "COMMENT_SEQ": "1",
            "CRT_TS": "2026-07-17 15:16:44",
            "COMMENTS": "test",
        },
    )

    class CompositeKeyOracle(FakeOracle):
        def find_transaction_tables(self, _xid: str, _pdb_name=None):
            return [TransactionTable(table, 1, 100, 101, None, None)]

        def get_table_metadata(self, _table: TableRef, _pdb_name=None):
            return COMPOSITE_KEY_METADATA

        def get_current_rows(self, metadata, keys, *_args):
            return {
                tuple(key[column] for column in metadata.key_columns): mined.after_delta
                for key in keys
            }

    config = ConnectorRuntimeConfig(
        topic_prefix="CDC.TOPO-CLI",
        include_patterns=(),
        exclude_patterns=(),
        connector_version="3.5.2.Final",
        key_schemas_enabled=True,
        decimal_handling_mode="double",
    )

    records, _ = Reconciler(CompositeKeyOracle([mined])).build_repairs(  # type: ignore[arg-type]
        AlertEvent("oracle", "01001000E6030000", datetime.now(timezone.utc), "log"),
        config,
    )

    assert records[0].key == {
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "type": "double",
                    "optional": False,
                    "default": 0.0,
                    "field": "ITEM_KEY",
                },
                {
                    "type": "double",
                    "optional": False,
                    "default": 0.0,
                    "field": "COMMENT_SEQ",
                },
                {
                    "type": "string",
                    "optional": False,
                    "default": "1970-01-01 00:00:00",
                    "field": "CRT_TS",
                },
            ],
            "optional": False,
            "name": "CDC.TOPO-CLI.TOPOVN.ITEM_COMMENTS.Key",
        },
        "payload": {
            "ITEM_KEY": 44576735.0,
            "COMMENT_SEQ": 1.0,
            "CRT_TS": "2026-07-17 15:16:44",
        },
    }
    assert records[0].value is not None
    assert records[0].value["after"]["ITEM_KEY"] == 44576735.0


def test_reconciler_rejects_partial_logminer_result() -> None:
    oracle = FakeOracle([change("INSERT", 1, "RID1", after={"ID": 1})])
    summaries = oracle.find_transaction_tables("xid")
    summaries[0] = TransactionTable(TABLE, 2, 100, 101, None, None)
    oracle.find_transaction_tables = lambda _xid, _pdb=None: summaries  # type: ignore[method-assign]
    event = AlertEvent("oracle", "01001000E6030000", datetime.now(timezone.utc), "log")

    with pytest.raises(IncompleteTransactionError, match="expected 2"):
        Reconciler(oracle).build_repairs(  # type: ignore[arg-type]
            event,
            ConnectorRuntimeConfig(
                "server", (), (), "ORCLCDB", connector_version="3.5.2.Final"
            ),
        )
