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
    connect_url: str
    config_db_host: str
    config_db_port: int
    config_db_name: str
    config_db_user: str
    config_db_password: str
    config_cache_ttl_seconds: float
    oracle_localhost_alias: str | None
    escalation_webhook_url: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            connect_url=_required("KAFKA_CONNECT_URL").rstrip("/"),
            config_db_host=_required("CONFIG_DB_HOST"),
            config_db_port=int(os.getenv("CONFIG_DB_PORT", "1433")),
            config_db_name=_required("CONFIG_DB_NAME"),
            config_db_user=_required("CONFIG_DB_USER"),
            config_db_password=_required("CONFIG_DB_PASSWORD"),
            config_cache_ttl_seconds=float(
                os.getenv("CONFIG_CACHE_TTL_SECONDS", "300")
            ),
            oracle_localhost_alias=(
                os.getenv("ORACLE_LOCALHOST_ALIAS", "").strip() or None
            ),
            escalation_webhook_url=os.getenv("ESCALATION_WEBHOOK_URL") or None,
        )
