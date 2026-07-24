"""Debezium Oracle abandoned-transaction remediation service."""
"""
Cấu trúc package:
- api: HTTP/webhook.
- application: business workflow.
- domain: model dữ liệu.
- oracle: Oracle và LogMiner.
- kafka: Kafka request/repair message.
- connect: Kafka Connect REST.
- support: config, logging và escalation.
"""
