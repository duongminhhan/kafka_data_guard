from __future__ import annotations

import re


_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")


def quote_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe Oracle identifier: {value!r}")
    return f'"{value}"'


def quote_qualified_name(value: str) -> str:
    return ".".join(quote_identifier(part) for part in value.split("."))

