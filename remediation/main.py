from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from remediation.config import Settings
from remediation.connect_client import ConnectClient
from remediation.escalation import EscalationClient
from remediation.kafka_publisher import KafkaPublisher
from remediation.log_parser import parse_alertmanager_payload
from remediation.logging_utils import configure_logging
from remediation.oracle_client import OracleClient
from remediation.reconciler import Reconciler
from remediation.request_queue import RequestConsumer, RequestPublisher, REQUEST_TOPIC
from remediation.service import RemediationService


@asynccontextmanager
async def lifespan(app: FastAPI):
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
