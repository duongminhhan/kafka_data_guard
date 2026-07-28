from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from src.kafka import publisher as kafka_publisher
from src.kafka import request_queue
from src.domain.models import AlertEvent, RepairRecord


class FakeProducer:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.calls: list[tuple[Any, ...]] = []
        self.flush_result = 0

    def init_transactions(self, timeout: float) -> None:
        self.calls.append(("init_transactions", timeout))

    def begin_transaction(self) -> None:
        self.calls.append(("begin_transaction",))

    def produce(self, **kwargs: Any) -> None:
        self.calls.append(("produce", kwargs))

    def commit_transaction(self, timeout: float) -> None:
        self.calls.append(("commit_transaction", timeout))

    def abort_transaction(self, timeout: float) -> None:
        self.calls.append(("abort_transaction", timeout))

    def flush(self, timeout: float) -> int:
        self.calls.append(("flush", timeout))
        return self.flush_result


def test_repair_publisher_relies_on_bounded_transaction_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_publisher, "Producer", FakeProducer)
    publisher = kafka_publisher.KafkaPublisher("kafka:9092")
    record = RepairRecord(
        topic="cdc.table",
        key={"ID": 1},
        value={"op": "c"},
        headers=(),
    )

    publisher.publish([record])

    producer = publisher._producer
    assert producer.config["transaction.timeout.ms"] == 60_000
    assert ("init_transactions", 30) in producer.calls
    assert ("commit_transaction", 30) in producer.calls
    assert not any(call[0] == "flush" for call in producer.calls)
    assert not any(call[0] == "poll" for call in producer.calls)


def test_request_publisher_uses_bounded_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(request_queue, "Producer", FakeProducer)
    publisher = request_queue.RequestPublisher("kafka:9092")
    event = AlertEvent(
        connector="oracle",
        xid="A1",
        detected_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
        log_line="abandoned",
    )

    publisher.publish([event])

    assert ("flush", 30) in publisher._producer.calls
    assert not any(call[0] == "poll" for call in publisher._producer.calls)


def test_request_publisher_reports_flush_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(request_queue, "Producer", FakeProducer)
    publisher = request_queue.RequestPublisher("kafka:9092")
    publisher._producer.flush_result = 1
    event = AlertEvent(
        connector="oracle",
        xid="A1",
        detected_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
        log_line="abandoned",
    )

    with pytest.raises(TimeoutError, match="1 remediation request"):
        publisher.publish([event])
