from __future__ import annotations

import re
import secrets
import threading
import time
import uuid
from dataclasses import dataclass

import httpx

from remediation.domain.models import TableRef


_RUNTIME_CONFIG_CACHE_TTL_SECONDS = 60.0


@dataclass(frozen=True)
class ConnectorRuntimeConfig:
    topic_prefix: str
    include_patterns: tuple[re.Pattern[str], ...]
    exclude_patterns: tuple[re.Pattern[str], ...]
    database_name: str | None = None
    pdb_name: str | None = None
    provide_transaction_metadata: bool = False
    connector_version: str | None = None
    include_redo_sql: bool = False
    connector_name: str = "oracle"
    task_id: str = "0"
    run_id: str | None = None
    key_schemas_enabled: bool = False
    decimal_handling_mode: str = "string"

    def includes(self, table: TableRef) -> bool:
        name = table.qualified_name
        included = not self.include_patterns or any(
            pattern.fullmatch(name) for pattern in self.include_patterns
        )
        excluded = any(pattern.fullmatch(name) for pattern in self.exclude_patterns)
        return included and not excluded


def _compile_list(value: str | None) -> tuple[re.Pattern[str], ...]:
    if not value:
        return ()
    return tuple(re.compile(item.strip()) for item in value.split(",") if item.strip())


def _uuid7() -> str:
    """Generate a UUIDv7 without requiring Python 3.14's uuid.uuid7()."""
    timestamp_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    random_a = secrets.randbits(12)
    random_b = secrets.randbits(62)
    value = (
        (timestamp_ms << 80)
        | (0x7 << 76)
        | (random_a << 64)
        | (0b10 << 62)
        | random_b
    )
    return str(uuid.UUID(int=value))


class ConnectClient:
    def __init__(
        self,
        base_url: str,
    ) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=10)
        self._run_id = _uuid7()
        self._cache_lock = threading.Lock()
        self._connector_locks: dict[str, threading.Lock] = {}
        self._runtime_config_cache: dict[
            str, tuple[float, ConnectorRuntimeConfig]
        ] = {}

    def get_runtime_config(self, connector: str) -> ConnectorRuntimeConfig:
        now = time.monotonic()
        with self._cache_lock:
            cached = self._runtime_config_cache.get(connector)
            if cached is not None and cached[0] > now:
                return cached[1]
            connector_lock = self._connector_locks.setdefault(
                connector, threading.Lock()
            )

        with connector_lock:
            now = time.monotonic()
            with self._cache_lock:
                cached = self._runtime_config_cache.get(connector)
                if cached is not None and cached[0] > now:
                    return cached[1]

            runtime_config = self._fetch_runtime_config(connector)
            with self._cache_lock:
                self._runtime_config_cache[connector] = (
                    time.monotonic() + _RUNTIME_CONFIG_CACHE_TTL_SECONDS,
                    runtime_config,
                )
            return runtime_config

    def list_connectors(self) -> list[str]:
        response = self._client.get("/connectors")
        response.raise_for_status()
        return sorted(str(item) for item in response.json())

    def _fetch_runtime_config(self, connector: str) -> ConnectorRuntimeConfig:
        response = self._client.get(f"/connectors/{connector}/config")
        response.raise_for_status()
        config = response.json()
        status_response = self._client.get(f"/connectors/{connector}/status")
        status_response.raise_for_status()
        status = status_response.json()
        connector_version = str(
            (status.get("connector") or {}).get("version") or ""
        ).strip()
        tasks = status.get("tasks") or []
        if not connector_version:
            connector_version = str(tasks[0].get("version") if tasks else "").strip()
        if not connector_version:
            raise ValueError("Kafka Connect status does not expose connector version")

        if str(config.get("transforms", "")).strip():
            raise ValueError(
                "Connector SMTs are configured; direct repair cannot guarantee the original topic/key/value contract"
            )
        converter = str(config.get("value.converter", "")).lower()
        if converter and "jsonconverter" not in converter:
            raise ValueError(
                "Only schemaless JSON CDC topics are supported; connector value.converter "
                f"is {config.get('value.converter')!r}"
            )
        schemas_enabled = str(
            config.get("value.converter.schemas.enable", "false")
        ).lower()
        if schemas_enabled == "true":
            raise ValueError("Schema-enabled JsonConverter is not supported safely")
        key_converter = str(config.get("key.converter", "")).lower()
        if key_converter and "jsonconverter" not in key_converter:
            raise ValueError(
                "Only schemaless JSON keys are supported; connector key.converter "
                f"is {config.get('key.converter')!r}"
            )
        key_schemas_enabled = str(
            config.get("key.converter.schemas.enable", "false")
        ).lower()
        decimal_mode = str(config.get("decimal.handling.mode", "precise")).lower()
        if decimal_mode not in {"string", "double"}:
            raise ValueError(
                "Connector decimal.handling.mode must be 'string' or 'double' for deterministic repair serialization"
            )
        time_mode = str(config.get("time.precision.mode", "adaptive")).lower()
        if time_mode != "connect":
            raise ValueError(
                "Connector time.precision.mode must be 'connect' for deterministic repair serialization"
            )

        topic_prefix = str(config.get("topic.prefix") or "").strip()
        if not topic_prefix:
            raise ValueError("Connector config has no topic.prefix")
        include = config.get("table.include.list")
        exclude = config.get("table.exclude.list")
        running_task = next(
            (task for task in tasks if str(task.get("state", "")).upper() == "RUNNING"),
            tasks[0] if tasks else {},
        )
        return ConnectorRuntimeConfig(
            topic_prefix=topic_prefix,
            include_patterns=_compile_list(include),
            exclude_patterns=_compile_list(exclude),
            database_name=str(config.get("database.dbname") or "").strip() or None,
            pdb_name=str(config.get("database.pdb.name") or "").strip() or None,
            provide_transaction_metadata=(
                str(config.get("provide.transaction.metadata", "false")).lower()
                == "true"
            ),
            connector_version=connector_version,
            include_redo_sql=(
                str(config.get("log.mining.include.redo.sql", "false")).lower()
                == "true"
            ),
            task_id=str(running_task.get("id", "0")),
            run_id=self._run_id,
            key_schemas_enabled=key_schemas_enabled == "true",
            decimal_handling_mode=decimal_mode,
        )

    def close(self) -> None:
        self._client.close()
