from __future__ import annotations

import re
from typing import Any

from remediation.models import TableMetadata


class LogMinerSqlParseError(RuntimeError):
    """LogMiner SQL_REDO/SQL_UNDO cannot be converted safely to column deltas."""


def _scan_parts(text: str, *, delimiter: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    single_quoted = False
    double_quoted = False
    index = 0
    upper = text.upper()
    keyword = delimiter.upper()

    while index < len(text):
        char = text[index]
        if single_quoted:
            if char == "'" and index + 1 < len(text) and text[index + 1] == "'":
                index += 2
                continue
            if char == "'":
                single_quoted = False
            index += 1
            continue
        if double_quoted:
            if char == '"' and index + 1 < len(text) and text[index + 1] == '"':
                index += 2
                continue
            if char == '"':
                double_quoted = False
            index += 1
            continue
        if char == "'":
            single_quoted = True
            index += 1
            continue
        if char == '"':
            double_quoted = True
            index += 1
            continue
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth -= 1
            index += 1
            continue

        if depth == 0 and upper.startswith(keyword, index):
            if delimiter == ",":
                parts.append(text[start:index].strip())
                start = index + 1
                index += 1
                continue
            before_ok = index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_")
            end = index + len(keyword)
            after_ok = end == len(text) or not (
                text[end].isalnum() or text[end] == "_"
            )
            if before_ok and after_ok:
                parts.append(text[start:index].strip())
                start = end
                index = end
                continue
        index += 1

    if single_quoted or double_quoted or depth != 0:
        raise LogMinerSqlParseError("Unbalanced quotes or parentheses in LogMiner SQL")
    parts.append(text[start:].strip())
    return [part for part in parts if part]


def _find_keyword(text: str, keyword: str) -> int:
    depth = 0
    single_quoted = False
    double_quoted = False
    upper = text.upper()
    target = keyword.upper()
    index = 0
    while index < len(text):
        char = text[index]
        if single_quoted:
            if char == "'" and index + 1 < len(text) and text[index + 1] == "'":
                index += 2
                continue
            if char == "'":
                single_quoted = False
            index += 1
            continue
        if double_quoted:
            if char == '"' and index + 1 < len(text) and text[index + 1] == '"':
                index += 2
                continue
            if char == '"':
                double_quoted = False
            index += 1
            continue
        if char == "'":
            single_quoted = True
            index += 1
            continue
        if char == '"':
            double_quoted = True
            index += 1
            continue
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth -= 1
            index += 1
            continue
        end = index + len(target)
        if (
            depth == 0
            and upper.startswith(target, index)
            and (index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_"))
            and (end == len(text) or not (text[end].isalnum() or text[end] == "_"))
        ):
            return index
        index += 1
    raise LogMinerSqlParseError(f"Missing {keyword} clause in LogMiner SQL")


def _canonical_column(identifier: str, metadata: TableMetadata) -> str | None:
    value = identifier.strip()
    if "." in value:
        value = value.rsplit(".", 1)[1]
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1].replace('""', '"')
    canonical = {column.upper(): column for column in metadata.columns}
    return canonical.get(value.upper())


def _first_function_argument(expression: str) -> str:
    opening = expression.find("(")
    if opening < 0 or not expression.rstrip().endswith(")"):
        raise LogMinerSqlParseError(f"Unsupported Oracle value: {expression}")
    arguments = _scan_parts(expression[opening + 1 : expression.rfind(")")], delimiter=",")
    if not arguments:
        raise LogMinerSqlParseError(f"Oracle function has no value: {expression}")
    return arguments[0]


def _parse_value(expression: str) -> Any:
    value = expression.strip().rstrip(";").strip()
    upper = value.upper()
    if upper == "NULL":
        return None
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        return value[1:-1].replace("''", "'")
    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:E[+-]?\d+)?", value, re.I):
        return value
    typed_literal = re.fullmatch(
        r"(?:DATE|TIMESTAMP(?:\s+WITH\s+TIME\s+ZONE)?)\s+'(.*)'",
        value,
        re.I | re.S,
    )
    if typed_literal:
        return typed_literal.group(1).replace("''", "'")
    function_name = value.split("(", 1)[0].strip().upper()
    if function_name in {
        "TO_DATE",
        "TO_TIMESTAMP",
        "TO_TIMESTAMP_TZ",
        "HEXTORAW",
        "UNISTR",
    }:
        parsed = _parse_value(_first_function_argument(value))
        if function_name == "HEXTORAW":
            try:
                return bytes.fromhex(str(parsed))
            except ValueError as exc:
                raise LogMinerSqlParseError(
                    f"Invalid HEXTORAW literal: {expression}"
                ) from exc
        if function_name == "UNISTR":
            return re.sub(
                r"\\([0-9A-Fa-f]{4})",
                lambda match: chr(int(match.group(1), 16)),
                str(parsed),
            )
        return parsed
    raise LogMinerSqlParseError(f"Unsupported Oracle value: {expression}")


def _parse_assignments(
    text: str, metadata: TableMetadata, *, separator: str
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for assignment in _scan_parts(text, delimiter=separator):
        is_null = re.fullmatch(r"(.+?)\s+IS\s+NULL", assignment, re.I | re.S)
        if is_null:
            column = _canonical_column(is_null.group(1), metadata)
            if column is not None:
                values[column] = None
            continue
        equality = _scan_parts(assignment, delimiter="=")
        if len(equality) != 2:
            raise LogMinerSqlParseError(
                f"Unsupported LogMiner predicate/assignment: {assignment}"
            )
        column = _canonical_column(equality[0], metadata)
        if column is not None:
            values[column] = _parse_value(equality[1])
    return values


def _parse_insert(sql: str, metadata: TableMetadata) -> dict[str, Any]:
    values_index = _find_keyword(sql, "VALUES")
    left = sql[:values_index].strip()
    right = sql[values_index + len("VALUES") :].strip().rstrip(";").strip()
    columns_open = left.rfind("(")
    columns_close = left.rfind(")")
    if columns_open < 0 or columns_close < columns_open:
        raise LogMinerSqlParseError("INSERT has no explicit column list")
    if not right.startswith("(") or not right.endswith(")"):
        raise LogMinerSqlParseError("INSERT has no VALUES list")
    columns = _scan_parts(left[columns_open + 1 : columns_close], delimiter=",")
    expressions = _scan_parts(right[1:-1], delimiter=",")
    if len(columns) != len(expressions):
        raise LogMinerSqlParseError("INSERT column/value count does not match")
    result: dict[str, Any] = {}
    for identifier, expression in zip(columns, expressions):
        column = _canonical_column(identifier, metadata)
        if column is not None:
            result[column] = _parse_value(expression)
    return result


def _parse_update(
    sql: str, metadata: TableMetadata
) -> tuple[dict[str, Any], dict[str, Any]]:
    set_index = _find_keyword(sql, "SET")
    where_index = _find_keyword(sql, "WHERE")
    if where_index <= set_index:
        raise LogMinerSqlParseError("UPDATE clauses are out of order")
    set_values = _parse_assignments(
        sql[set_index + len("SET") : where_index],
        metadata,
        separator=",",
    )
    where_values = _parse_assignments(
        sql[where_index + len("WHERE") :].rstrip(";"),
        metadata,
        separator="AND",
    )
    return set_values, where_values


def _parse_delete(sql: str, metadata: TableMetadata) -> dict[str, Any]:
    where_index = _find_keyword(sql, "WHERE")
    return _parse_assignments(
        sql[where_index + len("WHERE") :].rstrip(";"),
        metadata,
        separator="AND",
    )


def parse_logminer_change(
    operation: str,
    sql_redo: str | None,
    sql_undo: str | None,
    metadata: TableMetadata,
) -> tuple[dict[str, Any], dict[str, Any]]:
    operation = operation.upper()
    redo = (sql_redo or "").strip()
    undo = (sql_undo or "").strip()

    if operation == "INSERT":
        if not redo:
            raise LogMinerSqlParseError("INSERT has no SQL_REDO")
        return {}, _parse_insert(redo, metadata)

    if operation == "DELETE":
        if undo:
            return _parse_insert(undo, metadata), {}
        if redo:
            return _parse_delete(redo, metadata), {}
        raise LogMinerSqlParseError("DELETE has neither SQL_UNDO nor SQL_REDO")

    if operation == "UPDATE":
        if not redo:
            raise LogMinerSqlParseError("UPDATE has no SQL_REDO")
        redo_set, redo_where = _parse_update(redo, metadata)
        before = dict(redo_where)
        after: dict[str, Any] = {}
        if undo:
            undo_set, undo_where = _parse_update(undo, metadata)
            before.update(undo_set)
            after.update(undo_where)
        after.update(redo_set)
        for key_column in metadata.key_columns:
            if key_column in before and key_column not in after:
                after[key_column] = before[key_column]
        return before, after

    raise LogMinerSqlParseError(f"Unsupported LogMiner operation: {operation}")
