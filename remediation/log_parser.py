from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Iterable

from remediation.models import AlertEvent


# Oracle FLASHBACK_TRANSACTION_QUERY.XID is RAW(8), represented here as 16 hex chars.
_XID_PATTERNS = (
    re.compile(
        r"\b(?:xid|transaction(?:\s+id)?)\s*(?:=|:|is)?\s*"
        r"[\[('\"<]*?(?:0x)?(?P<xid>[0-9a-f]{16})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\btransaction\s+(?:with\s+id\s+)?(?:0x)?(?P<xid>[0-9a-f]{16})\b",
        re.IGNORECASE,
    ),
)

_EVENT_COUNT_PATTERN = re.compile(r"\b(?P<count>\d+)\s+events?\b", re.IGNORECASE)


def normalize_xid(value: str) -> str:
    xid = value.strip().removeprefix("0x").removeprefix("0X").upper()
    if not re.fullmatch(r"[0-9A-F]{16}", xid):
        raise ValueError(
            "XID must be the 16-hex-character RAW(8) representation expected by Oracle"
        )
    return xid


def extract_xid(log_line: str) -> str:
    if "abandon" not in log_line.lower():
        raise ValueError("Log line is not an abandoned-transaction message")
    for pattern in _XID_PATTERNS:
        match = pattern.search(log_line)
        if match:
            return normalize_xid(match.group("xid"))
    raise ValueError("Could not extract a RAW(8) XID from abandoned-transaction log")


def extract_event_count(log_line: str) -> int | None:
    match = _EVENT_COUNT_PATTERN.search(log_line)
    return int(match.group("count")) if match else None


def _first(mapping: dict[str, Any], names: Iterable[str]) -> str | None:
    for name in names:
        value = mapping.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _parse_time(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _context_value(
    labels: dict[str, Any], annotations: dict[str, Any], names: Iterable[str]
) -> str | None:
    return _first(labels, names) or _first(annotations, names)


def parse_alertmanager_payload(payload: dict[str, Any]) -> list[AlertEvent]:
    alerts = payload.get("alerts")
    if not isinstance(alerts, list):
        raise ValueError("Alertmanager payload must contain an alerts array")

    events: list[AlertEvent] = []
    seen: set[tuple[str, str]] = set()
    skipped_zero_events = 0
    for alert in alerts:
        labels = alert.get("labels") or {}
        annotations = alert.get("annotations") or {}
        connector = _first(
            labels,
            ("connector", "connector_name", "kafka_connect_connector", "name"),
        ) or _first(annotations, ("connector", "connector_name"))
        if not connector:
            raise ValueError("Alert is missing connector name")

        raw_xid = _first(labels, ("transaction_id", "xid")) or _first(
            annotations, ("transaction_id", "xid")
        )
        log_line = _first(
            annotations, ("log", "log_line", "message", "description", "summary")
        ) or ""
        raw_event_count = _first(labels, ("event_count", "events")) or _first(
            annotations, ("event_count", "events")
        )
        event_count = (
            int(raw_event_count)
            if raw_event_count is not None
            else extract_event_count(log_line)
        )
        if event_count == 0:
            skipped_zero_events += 1
            continue
        xid = normalize_xid(raw_xid) if raw_xid else extract_xid(log_line)
        dedupe_key = (connector, xid)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        events.append(
            AlertEvent(
                connector=connector,
                xid=xid,
                detected_at=_parse_time(alert.get("startsAt")),
                log_line=log_line,
                connector_name=_context_value(
                    labels,
                    annotations,
                    (
                        "__debezium.context.connectorName",
                        "debezium_connector_name",
                        "connector_type",
                    ),
                ),
                connector_logical_name=_context_value(
                    labels,
                    annotations,
                    (
                        "__debezium.context.connectorLogicalName",
                        "debezium_connector_logical_name",
                        "connector_logical_name",
                    ),
                ),
                task_id=_context_value(
                    labels,
                    annotations,
                    ("__debezium.context.taskId", "debezium_task_id", "task_id"),
                ),
                run_id=_context_value(
                    labels,
                    annotations,
                    ("__debezium.context.runId", "debezium_run_id", "run_id"),
                ),
            )
        )
    if not events and skipped_zero_events:
        return []
    if not events:
        raise ValueError("Payload contains no actionable abandoned transaction")
    return events
