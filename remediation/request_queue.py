from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from remediation.kafka_publisher import json_bytes
from remediation.models import AlertEvent
from remediation.service import RemediationService


logger = logging.getLogger(__name__)

REQUEST_TOPIC = "cdc-remediation-requests"
CONSUMER_GROUP = "debezium-oracle-remediation-v1"
PUBLISH_FLUSH_TIMEOUT_SECONDS = 30
CLOSE_FLUSH_TIMEOUT_SECONDS = 10
VIETNAM_TIMEZONE = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")


def event_to_request(event: AlertEvent) -> dict[str, str]:
    request = {
        "connector": event.connector,
        "transaction_id": event.xid,
        "detected_at": event.detected_at.astimezone(VIETNAM_TIMEZONE).isoformat(
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
    def __init__(self, bootstrap_servers: str) -> None:
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
                    topic=REQUEST_TOPIC,
                    key=f"{event.connector}:{event.xid}".encode(),
                    value=json_bytes(event_to_request(event)),
                    headers=[("source", "alertmanager")],
                    on_delivery=delivered,
                )
            outstanding = self._producer.flush(PUBLISH_FLUSH_TIMEOUT_SECONDS)
            if outstanding:
                raise TimeoutError(
                    f"Timed out publishing {outstanding} remediation request(s)"
                )
        if delivery_errors:
            raise RuntimeError("; ".join(delivery_errors))

    def close(self) -> None:
        outstanding = self._producer.flush(CLOSE_FLUSH_TIMEOUT_SECONDS)
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
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._service = service
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
        self._thread.join(timeout=15)

    def _run(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": CONSUMER_GROUP,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([REQUEST_TOPIC])
        logger.info(
            "Remediation request consumer started",
            extra={"topic": REQUEST_TOPIC, "consumer_group": CONSUMER_GROUP},
        )
        try:
            while not self._stop.is_set():
                message = consumer.poll(1)
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

                try:
                    self._service.remediate(event)
                    consumer.commit(message=message, asynchronous=False)
                    logger.info(
                        "Remediation request committed",
                        extra={
                            "connector": event.connector,
                            "xid": event.xid,
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
                            message.topic(), message.partition(), message.offset()
                        )
                    )
                    self._stop.wait(5)
        finally:
            consumer.close()
