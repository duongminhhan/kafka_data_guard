from __future__ import annotations

import threading

from src.configuration.models import OracleCredential
from src.oracle.client import OracleClient


class OracleClientRegistry:
    """Mỗi ConfigID có một Oracle client với pool một connection."""

    def __init__(
        self,
        pool_min: int,
        pool_max: int,
    ) -> None:
        self._pool_min = pool_min
        self._pool_max = pool_max
        self._lock = threading.Lock()
        self._clients: dict[int, tuple[OracleCredential, OracleClient]] = {}

    def get(self, config_id: int, credential: OracleCredential) -> OracleClient:
        with self._lock:
            cached = self._clients.get(config_id)
            if cached is None:
                client = OracleClient(
                    credential.username,
                    credential.password,
                    credential.dsn,
                    self._pool_min,
                    self._pool_max,
                )
                self._clients[config_id] = (credential, client)
            elif cached[0] != credential:
                cached[1].close()
                client = OracleClient(
                    credential.username,
                    credential.password,
                    credential.dsn,
                    self._pool_min,
                    self._pool_max,
                )
                self._clients[config_id] = (credential, client)
            return self._clients[config_id][1]

    def close(self) -> None:
        with self._lock:
            clients = [item[1] for item in self._clients.values()]
            self._clients.clear()
        for client in clients:
            client.close()
