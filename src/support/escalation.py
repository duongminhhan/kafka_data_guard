from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class EscalationClient:
    def __init__(
        self,
        webhook_url: str | None,
        http_timeout_seconds: float,
    ) -> None:
        self._webhook_url = webhook_url
        self._http_timeout_seconds = http_timeout_seconds

    def send(self, payload: dict[str, Any]) -> None:
        if not self._webhook_url:
            logger.error("Remediation escalation", extra=payload)
            return
        try:
            response = httpx.post(
                self._webhook_url,
                json=payload,
                timeout=self._http_timeout_seconds,
            )
            response.raise_for_status()
        except Exception:
            logger.exception("Failed to send remediation escalation")
