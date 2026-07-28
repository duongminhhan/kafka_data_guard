from __future__ import annotations

import logging
from typing import Any

from src.application.reconciler import Reconciler
from src.configuration.parsers import resolve_topic_bindings
from src.configuration.repository import GuardConfigCache
from src.connect.client import ConnectClient
from src.domain.models import AlertEvent
from src.kafka.publisher import KafkaPublisher
from src.oracle.registry import OracleClientRegistry
from src.support.escalation import EscalationClient

logger = logging.getLogger(__name__)


class RemediationService:
    def __init__(
        self,
        connect: ConnectClient,
        config_cache: GuardConfigCache,
        oracle_registry: OracleClientRegistry,
        publisher: KafkaPublisher,
        escalation: EscalationClient,
    ) -> None:
        self._connect = connect
        self._config_cache = config_cache
        self._oracle_registry = oracle_registry
        self._publisher = publisher
        self._escalation = escalation

    def remediate(self, event: AlertEvent) -> dict[str, Any]:
        """Gọi business logic rồi publish các repair record lên Kafka."""
        context = {"connector": event.connector, "xid": event.xid}
        try:
            logger.info("Remediation started", extra=context)
            guard_config = self._config_cache.get(event.connector)
            connector_config = self._connect.get_runtime_config(event.connector)
            topic_bindings = resolve_topic_bindings(
                guard_config.topics,
                connector_config,
                guard_config.credential.username,
            )
            topic_by_table = {
                binding.table: binding.full_topic for binding in topic_bindings
            }
            oracle = self._oracle_registry.get(
                guard_config.config_id,
                guard_config.credential,
            )
            records, stats = Reconciler(oracle).build_repairs(
                event,
                connector_config,
                topic_by_table,
            )
            # Chỉ service chính publish; các script trace dừng trước dòng này.
            self._publisher.publish(records)
            logger.info(
                "Remediation completed",
                extra={
                    **context,
                    "config_id": guard_config.config_id,
                    "status": "completed",
                },
            )
            return {
                **context,
                "config_id": guard_config.config_id,
                "status": "completed",
                **stats,
            }
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
