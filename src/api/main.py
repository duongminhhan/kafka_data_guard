from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from src.api.alert_parser import parse_alertmanager_payload
from src.application.service import RemediationService
from src.configuration.repository import GuardConfigCache, SqlServerConfigRepository
from src.connect.client import ConnectClient
from src.kafka.publisher import KafkaPublisher
from src.kafka.request_queue import RequestConsumer, RequestPublisher
from src.oracle.registry import OracleClientRegistry
from src.support.config import Settings
from src.support.escalation import EscalationClient
from src.support.logging_utils import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Khởi tạo một pipeline dùng chung cho cả webhook và Kafka request consumer.
    settings = Settings.from_env()
    configure_logging(settings.app_log_level)
    request_timezone = ZoneInfo(settings.request_timezone)
    config_repository = SqlServerConfigRepository(
        settings.config_db_host,
        settings.config_db_port,
        settings.config_db_name,
        settings.config_db_user,
        settings.config_db_password,
        settings.config_db_login_timeout_seconds,
        settings.config_db_query_timeout_seconds,
    )
    config_cache = GuardConfigCache(
        config_repository,
        settings.config_cache_ttl_seconds,
    )
    oracle_registry = OracleClientRegistry(
        settings.oracle_pool_min,
        settings.oracle_pool_max,
    )
    connect = ConnectClient(
        settings.connect_url,
        settings.connect_http_timeout_seconds,
        settings.connect_config_cache_ttl_seconds,
    )
    publisher = KafkaPublisher(
        settings.kafka_bootstrap_servers,
        settings.kafka_transactional_id,
        settings.kafka_transaction_timeout_ms,
        settings.kafka_transaction_api_timeout_seconds,
        settings.kafka_close_flush_timeout_seconds,
    )
    app.state.service = RemediationService(
        connect,
        config_cache,
        oracle_registry,
        publisher,
        EscalationClient(
            settings.escalation_webhook_url,
            settings.escalation_http_timeout_seconds,
        ),
    )
    app.state.request_topic = settings.kafka_request_topic
    app.state.request_publisher = RequestPublisher(
        settings.kafka_bootstrap_servers,
        settings.kafka_request_topic,
        request_timezone,
        settings.kafka_publish_flush_timeout_seconds,
        settings.kafka_close_flush_timeout_seconds,
    )
    app.state.request_consumer = RequestConsumer(
        settings.kafka_bootstrap_servers,
        app.state.service,
        settings.kafka_request_topic,
        settings.kafka_consumer_group,
        settings.kafka_max_poll_interval_ms,
        settings.kafka_consumer_poll_timeout_seconds,
        settings.kafka_retry_delay_seconds,
        settings.kafka_consumer_stop_timeout_seconds,
    )
    app.state.request_consumer.start()
    yield
    app.state.request_consumer.stop()
    app.state.request_publisher.close()
    publisher.close()
    connect.close()
    oracle_registry.close()


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
            "topic": request.app.state.request_topic,
            "requests": [],
        }

    try:
        # Consumer nền sẽ đọc request này và gọi RemediationService.
        request.app.state.request_publisher.publish(events)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Queue publish failed: {exc}") from exc
    return {
        "status": "queued",
        "topic": request.app.state.request_topic,
        "requests": [
            {"connector": event.connector, "xid": event.xid} for event in events
        ],
    }
