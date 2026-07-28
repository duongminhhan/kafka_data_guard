from __future__ import annotations

import threading

from src.configuration.models import OracleCredential
from src.oracle.client import OracleClient


class OracleClientRegistry:
    """Mỗi ConfigID có một Oracle client với pool một connection."""

    def __init__(
        self,
        localhost_alias: str | None,
        pool_min: int,
        pool_max: int,
    ) -> None:
        self._localhost_alias = localhost_alias
        self._pool_min = pool_min
        self._pool_max = pool_max
        self._lock = threading.Lock()
        self._clients: dict[int, tuple[OracleCredential, OracleClient]] = {}

    def get(self, config_id: int, credential: OracleCredential) -> OracleClient:
        with self._lock:
            cached = self._clients.get(config_id)
            if cached is None:
                dsn = self._dsn(credential)
                client = OracleClient(
                    credential.username,
                    credential.password,
                    dsn,
                    self._pool_min,
                    self._pool_max,
                )
                self._clients[config_id] = (credential, client)
            elif cached[0] != credential:
                cached[1].close()
                client = OracleClient(
                    credential.username,
                    credential.password,
                    self._dsn(credential),
                    self._pool_min,
                    self._pool_max,
                )
                self._clients[config_id] = (credential, client)
            return self._clients[config_id][1]

    def _dsn(self, credential: OracleCredential) -> str:
        host = credential.host
        if host.lower() in {"localhost", "127.0.0.1"} and self._localhost_alias:
            host = self._localhost_alias
        return f"{host}:{credential.port}/{credential.database}"

    def close(self) -> None:
        with self._lock:
            clients = [item[1] for item in self._clients.values()]
            self._clients.clear()
        for client in clients:
            client.close()
