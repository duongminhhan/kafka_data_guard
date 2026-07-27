from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from src.api.alert_parser import parse_alertmanager_payload
from src.application.reconciler import Reconciler
from src.application.service import RemediationService
from src.connect.client import ConnectClient
from src.kafka.publisher import KafkaPublisher
from src.kafka.request_queue import RequestConsumer, RequestPublisher, REQUEST_TOPIC
from src.oracle.client import OracleClient
from src.support.config import Settings
from src.support.escalation import EscalationClient
from src.support.logging_utils import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Khởi tạo một pipeline dùng chung cho cả webhook và Kafka request consumer.
    settings = Settings.from_env()
    configure_logging("INFO")
    oracle = OracleClient(
        settings.oracle_user,
        settings.oracle_password,
        settings.oracle_dsn,
        1,
        4,
    )
    connect = ConnectClient(settings.connect_url)
    publisher = KafkaPublisher(settings.kafka_bootstrap_servers)
    app.state.service = RemediationService(
        connect,
        Reconciler(oracle),
        publisher,
        EscalationClient(settings.escalation_webhook_url),
    )
    app.state.request_publisher = RequestPublisher(settings.kafka_bootstrap_servers)
    app.state.request_consumer = RequestConsumer(
        settings.kafka_bootstrap_servers,
        app.state.service,
    )
    app.state.request_consumer.start()
    yield
    app.state.request_consumer.stop()
    app.state.request_publisher.close()
    publisher.close()
    connect.close()
    oracle.close()


app = FastAPI(
    title="Debezium Oracle Abandoned Transaction Remediation",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhooks/alertmanager", status_code=202)
async def alertmanager_webhook(request: Request) -> dict[str, Any]:
    # Webhook chỉ parse alert và đẩy request lên Kafka, chưa query Oracle tại đây.
    try:
        events = parse_alertmanager_payload(await request.json())
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not events:
        return {
            "status": "ignored",
            "reason": "abandoned transaction contains 0 events",
            "topic": REQUEST_TOPIC,
            "requests": [],
        }

    try:
        # Consumer nền sẽ đọc request này và gọi RemediationService.
        request.app.state.request_publisher.publish(events)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Queue publish failed: {exc}") from exc
    return {
        "status": "queued",
        "topic": REQUEST_TOPIC,
        "requests": [
            {"connector": event.connector, "xid": event.xid} for event in events
        ],
    }
