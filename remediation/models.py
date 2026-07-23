from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class AlertEvent:
    connector: str
    xid: str
    detected_at: datetime
    log_line: str
    connector_name: str | None = None
    connector_logical_name: str | None = None
    task_id: str | None = None
    run_id: str | None = None


@dataclass(frozen=True)
class TableRef:
    owner: str
    name: str

    @property
    def qualified_name(self) -> str:
        return f"{self.owner}.{self.name}"


@dataclass(frozen=True)
class TransactionTable:
    table: TableRef
    change_count: int
    start_scn: int
    commit_scn: int | None
    start_time: datetime | None
    commit_time: datetime | None


@dataclass(frozen=True)
class TableMetadata:
    table: TableRef
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    column_types: dict[str, str]
    column_scales: dict[str, int | None]
    column_nullable: dict[str, bool] = field(default_factory=dict)
    column_defaults: dict[str, str | None] = field(default_factory=dict)


@dataclass(frozen=True)
class MinedChange:
    table: TableRef
    operation: str
    scn: int
    commit_scn: int
    rs_id: str
    ssn: int
    row_id: str | None
    redo_thread: int | None
    user_name: str | None
    change_time: datetime | None
    start_time: datetime | None
    commit_time: datetime | None
    redo_sql: str | None
    before_delta: dict[str, Any]
    after_delta: dict[str, Any]


@dataclass(frozen=True)
class ReplayEvent:
    table: TableRef
    operation: str
    scn: int
    commit_scn: int
    order: int
    change_time: datetime | None
    start_time: datetime | None
    commit_time: datetime | None
    rs_id: str
    ssn: int
    row_id: str | None
    redo_thread: int | None
    user_name: str | None
    redo_sql: str | None
    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None


@dataclass(frozen=True)
class RepairRecord:
    topic: str
    key: dict[str, Any]
    value: dict[str, Any] | None
    headers: tuple[tuple[str, str], ...]
