from __future__ import annotations

import pytest

from remediation.oracle.client import OracleClient
from remediation.oracle.sql_utils import quote_identifier, quote_qualified_name


def test_safe_oracle_identifiers_are_quoted() -> None:
    assert quote_qualified_name("APP.CUSTOMER") == '"APP"."CUSTOMER"'
    assert quote_identifier("CDC_TABLE$1") == '"CDC_TABLE$1"'


def test_unsafe_oracle_identifier_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsafe"):
        quote_qualified_name("APP.CUSTOMER; DROP TABLE X")


@pytest.mark.parametrize(
    ("value", "scale", "expected"),
    [
        ("63708542", 0, "63708542"),
        ("125.5", 2, "125.50"),
        ("1200", -2, "1200"),
        ("0.000001", None, "0.000001"),
    ],
)
def test_number_string_preserves_oracle_scale(
    value: str, scale: int | None, expected: str
) -> None:
    assert OracleClient._normalize_number(value, scale) == expected
