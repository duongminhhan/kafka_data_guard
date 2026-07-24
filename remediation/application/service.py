from __future__ import annotations

import logging
from typing import Any

from remediation.connect.client import ConnectClient
from remediation.support.escalation import EscalationClient
from remediation.kafka.publisher import KafkaPublisher
from remediation.domain.models import AlertEvent
from remediation.application.reconciler import Reconciler


logger = logging.getLogger(__name__)


class RemediationService:
    def __init__(
        self,
        connect: ConnectClient,
        reconciler: Reconciler,
        publisher: KafkaPublisher,
        escalation: EscalationClient,
    ) -> None:
        self._connect = connect
        self._reconciler = reconciler
        self._publisher = publisher
        self._escalation = escalation

    def remediate(self, event: AlertEvent) -> dict[str, Any]:
        """Gọi business logic rồi publish các repair record lên Kafka."""
        context = {"connector": event.connector, "xid": event.xid}
        try:
            logger.info("Remediation started", extra=context)
            connector_config = self._connect.get_runtime_config(event.connector)
            # Điểm vào của 7 bước remediation nằm trong build_repairs().
            records, stats = self._reconciler.build_repairs(event, connector_config)
            # Chỉ service chính publish; các script trace dừng trước dòng này.
            self._publisher.publish(records)
            logger.info(
                "Remediation completed", extra={**context, "status": "completed"}
            )
            return {**context, "status": "completed", **stats}
        except Exception as exc:
            details = f"{type(exc).__name__}: {exc}"
            logger.exception("Remediation failed", extra=context)
            self._escalation.send(
                {
                    "alert": "DebeziumAbandonedTransactionRemediationFailed",
                    **context,
                    "detected_at": event.detected_at.isoformat(),
                    "error": details,
                }
            )
            raise
