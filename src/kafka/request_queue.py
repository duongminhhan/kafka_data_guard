from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, tzinfo
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from src.application.service import RemediationService
from src.domain.models import AlertEvent
from src.kafka.publisher import json_bytes


logger = logging.getLogger(__name__)

def event_to_request(
    event: AlertEvent,
    request_timezone: tzinfo,
) -> dict[str, str]:
    request = {
        "connector": event.connector,
        "transaction_id": event.xid,
        "detected_at": event.detected_at.astimezone(request_timezone).isoformat(
            sep=" "
        ),
        "log_line": event.log_line,
    }
    optional_context = {
        "__debezium.context.connectorName": event.connector_name,
        "__debezium.context.connectorLogicalName": event.connector_logical_name,
        "__debezium.context.taskId": event.task_id,
        "__debezium.context.runId": event.run_id,
    }
    request.update(
        {key: value for key, value in optional_context.items() if value is not None}
    )
    return request


def request_to_event(payload: dict[str, Any]) -> AlertEvent:
    return AlertEvent(
        connector=str(payload["connector"]),
        xid=str(payload["transaction_id"]).upper(),
        detected_at=datetime.fromisoformat(
            str(payload["detected_at"]).replace("Z", "+00:00")
        ),
        log_line=str(payload["log_line"]),
        connector_name=(
            str(payload["__debezium.context.connectorName"])
            if payload.get("__debezium.context.connectorName") is not None
            else None
        ),
        connector_logical_name=(
            str(payload["__debezium.context.connectorLogicalName"])
            if payload.get("__debezium.context.connectorLogicalName") is not None
            else None
        ),
        task_id=(
            str(payload["__debezium.context.taskId"])
            if payload.get("__debezium.context.taskId") is not None
            else None
        ),
        run_id=(
            str(payload["__debezium.context.runId"])
            if payload.get("__debezium.context.runId") is not None
            else None
        ),
    )


class RequestPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        request_topic: str,
        request_timezone: tzinfo,
        publish_flush_timeout_seconds: float,
        close_flush_timeout_seconds: float,
    ) -> None:
        self._request_topic = request_topic
        self._request_timezone = request_timezone
        self._publish_flush_timeout_seconds = publish_flush_timeout_seconds
        self._close_flush_timeout_seconds = close_flush_timeout_seconds
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        self._lock = threading.Lock()

    def publish(self, events: list[AlertEvent]) -> None:
        delivery_errors: list[str] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))

        with self._lock:
            for event in events:
                self._producer.produce(
                    topic=self._request_topic,
                    # Cùng connector vào cùng partition để giữ thứ tự request.
                    key=event.connector.encode(),
                    value=json_bytes(event_to_request(event, self._request_timezone)),
                    headers=[("source", "alertmanager")],
                    on_delivery=delivered,
                )
            outstanding = self._producer.flush(
                self._publish_flush_timeout_seconds
            )
            if outstanding:
                raise TimeoutError(
                    f"Timed out publishing {outstanding} remediation request(s)"
                )
        if delivery_errors:
            raise RuntimeError("; ".join(delivery_errors))

    def close(self) -> None:
        outstanding = self._producer.flush(self._close_flush_timeout_seconds)
        if outstanding:
            logger.warning(
                "Kafka request publisher closed with %s outstanding message(s)",
                outstanding,
            )


class RequestConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        service: RemediationService,
        request_topic: str,
        consumer_group: str,
        max_poll_interval_ms: int,
        poll_timeout_seconds: float,
        retry_delay_seconds: float,
        stop_timeout_seconds: float,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._service = service
        self._request_topic = request_topic
        self._consumer_group = consumer_group
        self._max_poll_interval_ms = max_poll_interval_ms
        self._poll_timeout_seconds = poll_timeout_seconds
        self._retry_delay_seconds = retry_delay_seconds
        self._stop_timeout_seconds = stop_timeout_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="remediation-request-consumer",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._stop_timeout_seconds)

    def _run(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": self._consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                # Một transaction LogMiner có thể chạy lâu. Giữ consumer
                # trong group cho tới khi transaction đó xử lý xong.
                "max.poll.interval.ms": self._max_poll_interval_ms,
            }
        )
        consumer.subscribe([self._request_topic])
        logger.info(
            "Sequential remediation request consumer started",
            extra={
                "topic": self._request_topic,
                "consumer_group": self._consumer_group,
            },
        )
        try:
            while not self._stop.is_set():
                message = consumer.poll(self._poll_timeout_seconds)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka request consumer error: %s", message.error())
                    continue
                try:
                    payload = json.loads(message.value().decode("utf-8"))
                    event = request_to_event(payload)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    logger.exception(
                        "Discarding invalid remediation request",
                        extra={"topic": message.topic(), "offset": message.offset()},
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                # Chỉ poll request kế tiếp sau khi transaction hiện tại đã
                # xử lý thành công và commit offset.
                try:
                    result = self._service.remediate(event)
                    consumer.commit(message=message, asynchronous=False)
                    logger.info(
                        "Remediation request committed",
                        extra={
                            "connector": event.connector,
                            "xid": event.xid,
                            "config_id": result.get("config_id"),
                            "topic": message.topic(),
                            "offset": message.offset(),
                        },
                    )
                except Exception:
                    logger.exception(
                        "Remediation request will be retried",
                        extra={"connector": event.connector, "xid": event.xid},
                    )
                    consumer.seek(
                        TopicPartition(
                            message.topic(),
                            message.partition(),
                            message.offset(),
                        )
                    )
                    self._stop.wait(self._retry_delay_seconds)
        finally:
            consumer.close()
