from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from src.domain.models import AlertEvent
from src.kafka import request_queue as request_queue_module
from src.kafka.request_queue import event_to_request, request_to_event


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

    request = event_to_request(event, timezone(timedelta(hours=7)))
    assert request["detected_at"] == "2026-07-21 09:17:03+07:00"
    assert request["log_line"] == event.log_line
    assert request["__debezium.context.runId"] == event.run_id
    assert request_to_event(request) == event


def test_consumer_finishes_and_commits_before_processing_next_request(
    monkeypatch,
) -> None:
    events = [
        AlertEvent(
            connector="connector-a",
            xid=xid,
            detected_at=datetime(2026, 7, 28, tzinfo=timezone.utc),
            log_line=f"Transaction {xid} (5 events) is being abandoned.",
        )
        for xid in ("0100010001000000", "0200010001000000")
    ]
    actions: list[str] = []
    request_consumer = None

    class Message:
        def __init__(self, event: AlertEvent, offset: int) -> None:
            self.event = event
            self._offset = offset

        def value(self) -> bytes:
            return json.dumps(
                event_to_request(self.event, timezone.utc)
            ).encode()

        def error(self):
            return None

        def topic(self) -> str:
            return "KDG_REQUEST"

        def partition(self) -> int:
            return 0

        def offset(self) -> int:
            return self._offset

    class Consumer:
        def __init__(self, _config) -> None:
            self.messages = [Message(event, index) for index, event in enumerate(events)]

        def subscribe(self, _topics) -> None:
            return None

        def poll(self, _timeout):
            if self.messages:
                return self.messages.pop(0)
            request_consumer._stop.set()
            return None

        def commit(self, message, asynchronous=False) -> None:
            assert asynchronous is False
            actions.append(f"commit:{message.event.xid}")

        def close(self) -> None:
            return None

    class Service:
        def remediate(self, event: AlertEvent):
            actions.append(f"process:{event.xid}")
            return {"config_id": 1}

    monkeypatch.setattr(request_queue_module, "Consumer", Consumer)
    request_consumer = request_queue_module.RequestConsumer(
        "kafka:9092",
        Service(),
        "KDG_REQUEST",
        "APP_CDC_KDG",
        3_600_000,
        0.5,
        5,
        15,
    )
    request_consumer._run()

    assert actions == [
        f"process:{events[0].xid}",
        f"commit:{events[0].xid}",
        f"process:{events[1].xid}",
        f"commit:{events[1].xid}",
    ]
