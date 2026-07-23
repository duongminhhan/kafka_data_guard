from __future__ import annotations

from collections.abc import Callable
from typing import Any

from remediation.models import MinedChange, ReplayEvent, TableMetadata, TableRef


class IncompleteRedoError(RuntimeError):
    """Redo does not contain enough information to reproduce a full CDC event."""


SeedLoader = Callable[[TableMetadata, dict[str, Any]], dict[str, Any] | None]


def _key(values: dict[str, Any], columns: tuple[str, ...]) -> dict[str, Any]:
    missing = [column for column in columns if column not in values]
    if missing:
        raise IncompleteRedoError(
            f"LogMiner did not expose primary-key columns: {', '.join(missing)}"
        )
    return {column: values[column] for column in columns}


def reconstruct_events(
    changes: list[MinedChange],
    metadata_by_table: dict[TableRef, TableMetadata],
    seed_loader: SeedLoader,
) -> list[ReplayEvent]:
    """Apply ordered redo deltas and materialize full Debezium before/after images."""
    state: dict[tuple[TableRef, str], dict[str, Any]] = {}
    events: list[ReplayEvent] = []

    for order, change in enumerate(changes, start=1):
        metadata = metadata_by_table[change.table]
        operation = change.operation.upper()
        if operation not in {"INSERT", "UPDATE", "DELETE"}:
            raise ValueError(f"Unsupported LogMiner operation: {operation}")
        if not change.row_id:
            raise IncompleteRedoError(
                f"LogMiner returned no ROW_ID for {change.table.qualified_name}"
            )
        identity = (change.table, change.row_id)
        current = state.get(identity)

        if operation == "INSERT":
            if current is not None:
                raise IncompleteRedoError(
                    f"INSERT reused live ROW_ID {change.row_id} inside one transaction"
                )
            after = {column: None for column in metadata.columns}
            after.update(change.after_delta)
            event_key = _key(after, metadata.key_columns)
            before = None
            state[identity] = after.copy()

        elif operation == "UPDATE":
            if current is None:
                old_key = _key(change.before_delta, metadata.key_columns)
                current = seed_loader(metadata, old_key)
            if current is None:
                raise IncompleteRedoError(
                    f"Cannot reconstruct pre-transaction row for UPDATE {change.row_id}"
                )
            before = current.copy()
            before.update(change.before_delta)
            after = before.copy()
            after.update(change.after_delta)
            before_key = _key(before, metadata.key_columns)
            event_key = _key(after, metadata.key_columns)
            if before_key != event_key:
                raise IncompleteRedoError(
                    "Primary-key UPDATE needs Debezium key-change semantics and is not "
                    "safe for direct replay"
                )
            state[identity] = after.copy()

        else:
            if current is None:
                old_key = _key(change.before_delta, metadata.key_columns)
                current = seed_loader(metadata, old_key)
            if current is None:
                missing = [
                    column
                    for column in metadata.columns
                    if column not in change.before_delta
                ]
                if missing:
                    raise IncompleteRedoError(
                        f"Cannot reconstruct deleted row {change.row_id}; missing "
                        f"columns: {', '.join(missing)}"
                    )
            before = (current or {}).copy()
            before.update(change.before_delta)
            if not before:
                raise IncompleteRedoError(
                    f"Cannot reconstruct deleted row {change.row_id}"
                )
            event_key = _key(before, metadata.key_columns)
            after = None
            state.pop(identity, None)

        events.append(
            ReplayEvent(
                table=change.table,
                operation=operation,
                scn=change.scn,
                commit_scn=change.commit_scn,
                order=order,
                change_time=change.change_time,
                start_time=change.start_time,
                commit_time=change.commit_time,
                rs_id=change.rs_id,
                ssn=change.ssn,
                row_id=change.row_id,
                redo_thread=change.redo_thread,
                user_name=change.user_name,
                redo_sql=change.redo_sql,
                key=event_key,
                before=before,
                after=after,
            )
        )
    return events
