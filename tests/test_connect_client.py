from __future__ import annotations

import re
import threading
import time
from typing import Any

from src.connect import client as connect_client
from src.connect.client import ConnectClient, ConnectorRuntimeConfig
from src.domain.models import TableRef


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeHttpClient:
    def __init__(self, delay: float = 0) -> None:
        self.calls: list[str] = []
        self.delay = delay
        self._lock = threading.Lock()

    def get(self, path: str) -> _Response:
        with self._lock:
            self.calls.append(path)
        if self.delay:
            time.sleep(self.delay)
        if path.endswith("/config"):
            connector = path.split("/")[2]
            return _Response(
                {
                    "topic.prefix": connector,
                    "value.converter": "JsonConverter",
                    "key.converter": "JsonConverter",
                    "decimal.handling.mode": "string",
                    "time.precision.mode": "connect",
                }
            )
        return _Response(
            {
                "connector": {"version": "3.2.0"},
                "tasks": [{"id": 0, "state": "RUNNING"}],
            }
        )

    def close(self) -> None:
        pass


def _client(fake_http_client: _FakeHttpClient) -> ConnectClient:
    client = ConnectClient(
        "http://connect:8083",
        http_timeout_seconds=10,
        cache_ttl_seconds=60,
    )
    client._client.close()
    client._client = fake_http_client  # type: ignore[assignment]
    return client


def test_table_include_and_exclude_are_full_match_patterns() -> None:
    config = ConnectorRuntimeConfig(
        topic_prefix="server1",
        include_patterns=(re.compile(r"APP\..*"),),
        exclude_patterns=(re.compile(r"APP\.AUDIT"),),
    )
    assert config.includes(TableRef("APP", "CUSTOMER"))
    assert not config.includes(TableRef("APP", "AUDIT"))
    assert not config.includes(TableRef("OTHER", "CUSTOMER"))


def test_runtime_config_is_cached_per_connector() -> None:
    http_client = _FakeHttpClient()
    client = _client(http_client)

    first = client.get_runtime_config("oracle-a")
    second = client.get_runtime_config("oracle-a")
    other = client.get_runtime_config("oracle-b")

    assert second is first
    assert other.topic_prefix == "oracle-b"
    assert http_client.calls == [
        "/connectors/oracle-a/config",
        "/connectors/oracle-a/status",
        "/connectors/oracle-b/config",
        "/connectors/oracle-b/status",
    ]


def test_runtime_config_is_refetched_after_ttl(monkeypatch: Any) -> None:
    http_client = _FakeHttpClient()
    client = _client(http_client)
    monotonic_values = iter((100.0, 100.0, 100.0, 161.0, 161.0, 161.0))
    monkeypatch.setattr(connect_client.time, "monotonic", lambda: next(monotonic_values))

    first = client.get_runtime_config("oracle")
    second = client.get_runtime_config("oracle")

    assert second is not first
    assert http_client.calls.count("/connectors/oracle/config") == 2
    assert http_client.calls.count("/connectors/oracle/status") == 2


def test_concurrent_calls_only_fetch_runtime_config_once() -> None:
    http_client = _FakeHttpClient(delay=0.01)
    client = _client(http_client)
    results: list[ConnectorRuntimeConfig] = []
    start = threading.Barrier(5)

    def load_config() -> None:
        start.wait()
        results.append(client.get_runtime_config("oracle"))

    threads = [threading.Thread(target=load_config) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 5
    assert all(result is results[0] for result in results)
    assert http_client.calls == [
        "/connectors/oracle/config",
        "/connectors/oracle/status",
    ]
