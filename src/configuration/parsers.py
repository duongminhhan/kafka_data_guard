from __future__ import annotations

import re
from typing import Any, Iterable

from src.configuration.models import (
    OracleCredential,
    RawTopicBinding,
    TopicBinding,
)
from src.connect.client import ConnectorRuntimeConfig
from src.domain.models import TableRef


def sanitize_topic_component(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def parse_oracle_credential(value: str) -> OracleCredential:
    """Parse contract: protocol;host:port;database;user;password;type."""
    parts = value.split(";")
    if len(parts) != 6:
        raise ValueError("Oracle credential must contain exactly 6 fields")
    protocol, endpoint, database, username, password, connection_type = (
        part.strip() for part in parts
    )
    if protocol != "jdbc:oracle:thin:@//":
        raise ValueError("Unsupported Oracle JDBC protocol")
    if ":" not in endpoint:
        raise ValueError("Oracle endpoint must use host:port")
    host, port_text = endpoint.rsplit(":", 1)
    if not all((host, port_text, database, username, password, connection_type)):
        raise ValueError("Oracle credential contains an empty required field")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError("Oracle port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("Oracle port is outside 1..65535")
    if connection_type not in {"normal_type"}:
        raise ValueError("Unsupported Oracle connection type")
    return OracleCredential(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_type=connection_type,
    )


def parse_topic_list(value: Any) -> tuple[RawTopicBinding, ...]:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("ListCDCTopic must be a non-empty CSV string")
    decoded = [item.strip() for item in value.split(",")]
    if any(not item for item in decoded):
        raise ValueError("ListCDCTopic contains an empty CSV item")
    bindings: list[RawTopicBinding] = []
    seen: set[str] = set()
    for item in decoded:
        full_topic = item
        try:
            prefix, schema, table = full_topic.rsplit(".", 2)
        except ValueError as exc:
            raise ValueError(f"CDC topic has fewer than 3 parts: {full_topic}") from exc
        if not all((prefix, schema, table)):
            raise ValueError(f"CDC topic contains an empty component: {full_topic}")
        if full_topic in seen:
            raise ValueError(f"Duplicate CDC topic: {full_topic}")
        seen.add(full_topic)
        bindings.append(RawTopicBinding(full_topic, prefix, schema, table))
    return tuple(bindings)


def resolve_topic_bindings(
    raw_topics: Iterable[RawTopicBinding],
    connector_config: ConnectorRuntimeConfig,
    oracle_username: str,
) -> tuple[TopicBinding, ...]:
    """Map sanitized topic schema về owner thật, fail nếu mapping mơ hồ."""
    exact_tables = connector_config.included_tables
    resolved: list[TopicBinding] = []
    for topic in raw_topics:
        candidates = [
            table
            for table in exact_tables
            if table.name == topic.table_name
            and sanitize_topic_component(table.owner) == topic.topic_schema
        ]
        if not candidates:
            username_table = TableRef(oracle_username.upper(), topic.table_name)
            if sanitize_topic_component(username_table.owner) == topic.topic_schema:
                candidates = [username_table]
        if len(candidates) != 1:
            raise ValueError(
                f"Cannot uniquely map CDC topic to Oracle table: {topic.full_topic}"
            )
        table = candidates[0]
        if not connector_config.includes(table):
            raise ValueError(f"CDC topic is outside connector table scope: {topic.full_topic}")
        resolved.append(
            TopicBinding(
                topic.full_topic,
                topic.topic_prefix,
                topic.topic_schema,
                table,
            )
        )
    return tuple(resolved)
