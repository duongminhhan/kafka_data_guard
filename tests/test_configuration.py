from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.configuration.models import GuardConfig
from src.configuration.parsers import (
    parse_oracle_credential,
    parse_topic_list,
    resolve_topic_bindings,
)
from src.configuration.repository import GuardConfigCache
from src.connect.client import ConnectorRuntimeConfig
from src.domain.models import TableRef
from src.oracle import registry as registry_module


def test_parse_oracle_credential_contract() -> None:
    credential = parse_oracle_credential(
        "jdbc:oracle:thin:@//;localhost:1521;ORCLCDB;C##CDCUSER;secret;normal_type"
    )

    assert credential.host == "localhost"
    assert credential.port == 1521
    assert credential.database == "ORCLCDB"
    assert credential.username == "C##CDCUSER"
    assert credential.password == "secret"
    assert credential.dsn == "localhost:1521/ORCLCDB"


@pytest.mark.parametrize(
    "value",
    [
        "jdbc:oracle:thin:@//;localhost;ORCLCDB;user;pwd;normal_type",
        "jdbc:oracle:thin:@//;localhost:abc;ORCLCDB;user;pwd;normal_type",
        "jdbc:postgresql://;localhost:1521;ORCLCDB;user;pwd;normal_type",
        "jdbc:oracle:thin:@//;localhost:1521;ORCLCDB;user;pwd",
    ],
)
def test_parse_oracle_credential_rejects_invalid_contract(value: str) -> None:
    with pytest.raises(ValueError):
        parse_oracle_credential(value)


def test_topic_parser_strips_prefix_from_the_right() -> None:
    topic = parse_topic_list(
        "company.oracle.prefix.C__CDCUSER.PRODUCT"
    )[0]

    assert topic.topic_prefix == "company.oracle.prefix"
    assert topic.topic_schema == "C__CDCUSER"
    assert topic.table_name == "PRODUCT"


def test_topic_parser_reads_comma_separated_topics() -> None:
    topics = parse_topic_list(
        "oracle_remediation_poc.C__CDCUSER.PRODUCT, "
        "oracle_remediation_poc.C__CDCUSER.ITEM"
    )

    assert [topic.table_name for topic in topics] == ["PRODUCT", "ITEM"]


def test_topic_binding_uses_real_owner_from_connector_metadata() -> None:
    raw = parse_topic_list(
        "oracle_remediation_poc.C__CDCUSER.PRODUCT"
    )
    table = TableRef("C##CDCUSER", "PRODUCT")
    connector = ConnectorRuntimeConfig(
        topic_prefix="oracle_remediation_poc",
        include_patterns=(),
        exclude_patterns=(),
        included_tables=(table,),
    )

    binding = resolve_topic_bindings(raw, connector, "C##CDCUSER")[0]

    assert binding.full_topic == raw[0].full_topic
    assert binding.table == table


def test_config_cache_does_not_reload_each_request() -> None:
    now = datetime.now(timezone.utc)
    config = GuardConfig(
        connector_name="oracle-remediation-poc",
        config_id=1,
        database_type="oracle",
        credential=parse_oracle_credential(
            "jdbc:oracle:thin:@//;localhost:1521;ORCLCDB;user;pwd;normal_type"
        ),
        topics=parse_topic_list("prefix.USER.TABLE_A"),
        updated_at=now,
    )

    class Repository:
        calls = 0

        def get_by_connector(self, _connector_name: str) -> GuardConfig:
            self.calls += 1
            return config

    repository = Repository()
    cache = GuardConfigCache(repository, ttl_seconds=60)  # type: ignore[arg-type]

    assert cache.get("oracle-remediation-poc") is config
    assert cache.get("oracle-remediation-poc") is config
    assert repository.calls == 1


def test_oracle_registry_isolates_clients_by_config_id(monkeypatch) -> None:
    created: list[tuple[str, str, str, int, int]] = []

    class FakeOracleClient:
        def __init__(self, user, password, dsn, pool_min, pool_max) -> None:
            created.append((user, password, dsn, pool_min, pool_max))

        def close(self) -> None:
            return None

    monkeypatch.setattr(registry_module, "OracleClient", FakeOracleClient)
    registry = registry_module.OracleClientRegistry(
        pool_min=1,
        pool_max=1,
    )
    first = parse_oracle_credential(
        "jdbc:oracle:thin:@//;localhost:1521;ORCLCDB;user_a;pwd;normal_type"
    )
    second = parse_oracle_credential(
        "jdbc:oracle:thin:@//;server-b:1521;UATB;user_b;pwd;normal_type"
    )

    client_a = registry.get(1, first)
    assert registry.get(1, first) is client_a
    client_b = registry.get(2, second)

    assert client_a is not client_b
    assert created == [
        ("user_a", "pwd", "localhost:1521/ORCLCDB", 1, 1),
        ("user_b", "pwd", "server-b:1521/UATB", 1, 1),
    ]
