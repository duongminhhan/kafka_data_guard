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
    oracle_dsn: str
    oracle_user: str
    oracle_password: str
    kafka_bootstrap_servers: str
    connect_url: str
    escalation_webhook_url: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            oracle_dsn=_required("ORACLE_DSN"),
            oracle_user=_required("ORACLE_USER"),
            oracle_password=_required("ORACLE_PASSWORD"),
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            connect_url=_required("KAFKA_CONNECT_URL").rstrip("/"),
            escalation_webhook_url=os.getenv("ESCALATION_WEBHOOK_URL") or None,
        )
