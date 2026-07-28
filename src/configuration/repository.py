from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Callable

from src.configuration.models import GuardConfig
from src.configuration.parsers import parse_oracle_credential, parse_topic_list


class ConfigNotFoundError(LookupError):
    pass


class SqlServerConfigRepository:
    """Chỉ gọi stored procedure; không SELECT trực tiếp bảng credential."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        login_timeout_seconds: int,
        query_timeout_seconds: int,
        connect_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._connection_args = {
            "server": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "login_timeout": login_timeout_seconds,
            "timeout": query_timeout_seconds,
        }
        self._connect_factory = connect_factory

    def _connect(self) -> Any:
        if self._connect_factory is None:
            import pymssql

            return pymssql.connect(**self._connection_args, as_dict=True)
        return self._connect_factory(**self._connection_args, as_dict=True)

    def get_by_connector(self, connector_name: str) -> GuardConfig:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.callproc(
                    "dbo.spGetKafkaGuardTopicConfig",
                    (connector_name,),
                )
                rows = list(cursor)
        if not rows:
            raise ConfigNotFoundError(
                f"No enabled Kafka Guard config for connector {connector_name!r}"
            )
        if len(rows) != 1:
            raise ValueError(
                f"Expected one Kafka Guard config for connector {connector_name!r}"
            )
        row = rows[0]
        database_type = str(row["DatabaseType"]).strip().lower()
        if database_type != "oracle":
            raise ValueError(f"Unsupported source database type: {database_type}")
        return GuardConfig(
            connector_name=str(row["ConnectorName"]),
            config_id=int(row["ConfigID"]),
            database_type=database_type,
            credential=parse_oracle_credential(str(row["DatabaseCredential"])),
            topics=parse_topic_list(row["ListCDCTopic"]),
            updated_at=_datetime(row.get("UpdatedAt")),
        )


def _datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


class GuardConfigCache:
    def __init__(
        self,
        repository: SqlServerConfigRepository,
        ttl_seconds: float,
    ) -> None:
        self._repository = repository
        self._ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[float, GuardConfig]] = {}
        self._connector_locks: dict[str, threading.Lock] = {}

    def get(self, connector_name: str) -> GuardConfig:
        now = time.monotonic()
        with self._lock:
            cached = self._entries.get(connector_name)
            if cached and cached[0] > now:
                return cached[1]
            load_lock = self._connector_locks.setdefault(
                connector_name, threading.Lock()
            )
        with load_lock:
            now = time.monotonic()
            with self._lock:
                cached = self._entries.get(connector_name)
                if cached and cached[0] > now:
                    return cached[1]
            config = self._repository.get_by_connector(connector_name)
            with self._lock:
                self._entries[connector_name] = (
                    time.monotonic() + self._ttl_seconds,
                    config,
                )
            return config

    def invalidate(self, connector_name: str) -> None:
        with self._lock:
            self._entries.pop(connector_name, None)
