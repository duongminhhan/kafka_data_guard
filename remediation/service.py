from __future__ import annotations

import logging
from typing import Any

from remediation.connect_client import ConnectClient
from remediation.escalation import EscalationClient
from remediation.kafka_publisher import KafkaPublisher
from remediation.models import AlertEvent
from remediation.reconciler import Reconciler


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
        context = {"connector": event.connector, "xid": event.xid}
        try:
            logger.info("Remediation started", extra=context)
            connector_config = self._connect.get_runtime_config(event.connector)
            records, stats = self._reconciler.build_repairs(event, connector_config)
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
