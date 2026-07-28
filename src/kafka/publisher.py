from __future__ import annotations

import base64
import json
import threading
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from confluent_kafka import Producer
from src.domain.models import RepairRecord

def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(aware.timestamp() * 1000)
    if isinstance(value, date):
        return (value - date(1970, 1, 1)).days
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    raise TypeError(f"Value of type {type(value).__name__} is not JSON serializable")


def json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        transactional_id: str,
        transaction_timeout_ms: int,
        transaction_api_timeout_seconds: float,
        close_flush_timeout_seconds: float,
    ) -> None:
        self._transaction_api_timeout_seconds = transaction_api_timeout_seconds
        self._close_flush_timeout_seconds = close_flush_timeout_seconds
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "transactional.id": transactional_id,
                "transaction.timeout.ms": transaction_timeout_ms,
            }
        )
        self._lock = threading.Lock()
        self._producer.init_transactions(self._transaction_api_timeout_seconds)

    def publish(self, records: list[RepairRecord]) -> None:
        if not records:
            return

        with self._lock:
            self._producer.begin_transaction()
            try:
                for record in records:
                    self._producer.produce(
                        topic=record.topic,
                        key=json_bytes(record.key),
                        value=json_bytes(record.value) if record.value is not None else None,
                        headers=list(record.headers),
                    )
                # commit_transaction flushes outstanding messages and serves
                # delivery failures before completing the transaction.
                self._producer.commit_transaction(
                    self._transaction_api_timeout_seconds
                )
            except Exception:
                self._producer.abort_transaction(
                    self._transaction_api_timeout_seconds
                )
                raise

    def close(self) -> None:
        self._producer.flush(self._close_flush_timeout_seconds)
