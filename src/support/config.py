from __future__ import annotations

import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_request_topic: str
    kafka_consumer_group: str
    kafka_transactional_id: str
    kafka_transaction_timeout_ms: int
    kafka_transaction_api_timeout_seconds: float
    kafka_publish_flush_timeout_seconds: float
    kafka_close_flush_timeout_seconds: float
    kafka_max_poll_interval_ms: int
    kafka_consumer_poll_timeout_seconds: float
    kafka_retry_delay_seconds: float
    kafka_consumer_stop_timeout_seconds: float
    connect_url: str
    connect_http_timeout_seconds: float
    connect_config_cache_ttl_seconds: float
    config_db_host: str
    config_db_port: int
    config_db_name: str
    config_db_user: str
    config_db_password: str
    config_db_login_timeout_seconds: int
    config_db_query_timeout_seconds: int
    config_cache_ttl_seconds: float
    oracle_pool_min: int
    oracle_pool_max: int
    escalation_webhook_url: str | None
    escalation_http_timeout_seconds: float
    request_timezone: str
    app_log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_request_topic=_required("KAFKA_REQUEST_TOPIC"),
            kafka_consumer_group=_required("KAFKA_CONSUMER_GROUP"),
            kafka_transactional_id=_required("KAFKA_TRANSACTIONAL_ID"),
            kafka_transaction_timeout_ms=int(
                _required("KAFKA_TRANSACTION_TIMEOUT_MS")
            ),
            kafka_transaction_api_timeout_seconds=float(
                _required("KAFKA_TRANSACTION_API_TIMEOUT_SECONDS")
            ),
            kafka_publish_flush_timeout_seconds=float(
                _required("KAFKA_PUBLISH_FLUSH_TIMEOUT_SECONDS")
            ),
            kafka_close_flush_timeout_seconds=float(
                _required("KAFKA_CLOSE_FLUSH_TIMEOUT_SECONDS")
            ),
            kafka_max_poll_interval_ms=int(
                _required("KAFKA_MAX_POLL_INTERVAL_MS")
            ),
            kafka_consumer_poll_timeout_seconds=float(
                _required("KAFKA_CONSUMER_POLL_TIMEOUT_SECONDS")
            ),
            kafka_retry_delay_seconds=float(
                _required("KAFKA_RETRY_DELAY_SECONDS")
            ),
            kafka_consumer_stop_timeout_seconds=float(
                _required("KAFKA_CONSUMER_STOP_TIMEOUT_SECONDS")
            ),
            connect_url=_required("KAFKA_CONNECT_URL").rstrip("/"),
            connect_http_timeout_seconds=float(
                _required("KAFKA_CONNECT_HTTP_TIMEOUT_SECONDS")
            ),
            connect_config_cache_ttl_seconds=float(
                _required("KAFKA_CONNECT_CONFIG_CACHE_TTL_SECONDS")
            ),
            config_db_host=_required("CONFIG_DB_HOST"),
            config_db_port=int(os.getenv("CONFIG_DB_PORT", "1433")),
            config_db_name=_required("CONFIG_DB_NAME"),
            config_db_user=_required("CONFIG_DB_USER"),
            config_db_password=_required("CONFIG_DB_PASSWORD"),
            config_db_login_timeout_seconds=int(
                _required("CONFIG_DB_LOGIN_TIMEOUT_SECONDS")
            ),
            config_db_query_timeout_seconds=int(
                _required("CONFIG_DB_QUERY_TIMEOUT_SECONDS")
            ),
            config_cache_ttl_seconds=float(
                _required("CONFIG_CACHE_TTL_SECONDS")
            ),
            oracle_pool_min=int(_required("ORACLE_POOL_MIN")),
            oracle_pool_max=int(_required("ORACLE_POOL_MAX")),
            escalation_webhook_url=os.getenv("ESCALATION_WEBHOOK_URL") or None,
            escalation_http_timeout_seconds=float(
                _required("ESCALATION_HTTP_TIMEOUT_SECONDS")
            ),
            request_timezone=_required("REQUEST_TIMEZONE"),
            app_log_level=_required("APP_LOG_LEVEL"),
        )
