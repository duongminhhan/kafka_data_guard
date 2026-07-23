from __future__ import annotations

import pytest

from remediation.log_parser import (
    extract_event_count,
    extract_xid,
    normalize_xid,
    parse_alertmanager_payload,
)


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (
            "Transaction 0a00120034000000 has been abandoned because it exceeded retention",
            "0A00120034000000",
        ),
        (
            "Oracle transaction id 'AABBCCDDEEFF0011' was ABANDONED by the buffer",
            "AABBCCDDEEFF0011",
        ),
        (
            "Abandoned transaction detected: XID=0102030405060708",
            "0102030405060708",
        ),
    ],
)
def test_extract_xid_supported_log_variants(line: str, expected: str) -> None:
    assert extract_xid(line) == expected


def test_extract_xid_rejects_unrelated_log() -> None:
    with pytest.raises(ValueError, match="not an abandoned"):
        extract_xid("Transaction 0A00120034000000 committed")


def test_normalize_xid_rejects_non_raw8_identifier() -> None:
    with pytest.raises(ValueError, match="16-hex-character"):
        normalize_xid("12.34.56")


def test_extract_event_count_from_full_debezium_log() -> None:
    line = (
        "Transaction 02001600a8030000 (start SCN 6178327, change time "
        "2026-07-21T03:06:20Z, redo thread 1, 5 events) is being abandoned."
    )
    assert extract_event_count(line) == 5


def test_parse_alertmanager_payload_bypasses_zero_event_transaction() -> None:
    payload = {
        "alerts": [
            {
                "labels": {
                    "connector": "oracle-cdc",
                    "transaction_id": "02001600a8030000",
                    "event_count": "0",
                },
                "annotations": {
                    "log_line": (
                        "Transaction 02001600a8030000 (start SCN 6178327, "
                        "change time 2026-07-21T03:06:20Z, redo thread 1, "
                        "0 events) is being abandoned."
                    )
                },
            }
        ]
    }
    assert parse_alertmanager_payload(payload) == []


def test_parse_alertmanager_payload_prefers_explicit_labels_and_deduplicates() -> None:
    payload = {
        "alerts": [
            {
                "labels": {
                    "connector": "oracle-cdc",
                    "transaction_id": "0a00120034000000",
                },
                "annotations": {"description": "transaction abandoned"},
                "startsAt": "2026-07-20T08:00:00Z",
            },
            {
                "labels": {"connector": "oracle-cdc"},
                "annotations": {
                    "log_line": "Transaction 0A00120034000000 was abandoned"
                },
            },
        ]
    }
    events = parse_alertmanager_payload(payload)
    assert len(events) == 1
    assert events[0].connector == "oracle-cdc"
    assert events[0].xid == "0A00120034000000"


def test_parse_alertmanager_payload_keeps_debezium_header_context() -> None:
    payload = {
        "alerts": [
            {
                "labels": {
                    "connector": "oracle-cdc",
                    "transaction_id": "0a00120034000000",
                    "debezium_connector_name": "oracle",
                    "debezium_connector_logical_name": "CDC.TOPO-CLI",
                    "debezium_task_id": "0",
                    "debezium_run_id": "019f6368-c503-7d1e-8d07-3ff4d6dafdd1",
                },
                "annotations": {"description": "transaction abandoned"},
            }
        ]
    }

    event = parse_alertmanager_payload(payload)[0]
    assert event.connector_name == "oracle"
    assert event.connector_logical_name == "CDC.TOPO-CLI"
    assert event.task_id == "0"
    assert event.run_id == "019f6368-c503-7d1e-8d07-3ff4d6dafdd1"
