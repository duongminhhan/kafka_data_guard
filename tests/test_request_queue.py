from __future__ import annotations

from datetime import datetime, timezone

from remediation.models import AlertEvent
from remediation.request_queue import event_to_request, request_to_event


def test_request_message_round_trip() -> None:
    event = AlertEvent(
        connector="oracle-remediation-poc",
        xid="08001D00CE030000",
        detected_at=datetime(2026, 7, 21, 2, 17, 3, tzinfo=timezone.utc),
        log_line=(
            "Transaction 08001d00ce030000 (start SCN 6165153, change time "
            "2026-07-21T02:15:59Z, redo thread 1, 5 events) is being abandoned."
        ),
        connector_name="oracle",
        connector_logical_name="CDC.TOPO-CLI",
        task_id="0",
        run_id="019f6368-c503-7d1e-8d07-3ff4d6dafdd1",
    )

    request = event_to_request(event)
    assert request["detected_at"] == "2026-07-21 09:17:03+07:00"
    assert request["log_line"] == event.log_line
    assert request["__debezium.context.runId"] == event.run_id
    assert request_to_event(request) == event
