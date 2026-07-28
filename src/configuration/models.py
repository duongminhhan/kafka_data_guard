from __future__ import annotations

from dataclasses import dataclass

from src.domain.models import TableRef


@dataclass(frozen=True)
class OracleCredential:
    """Credential Oracle đã parse; raw connection string không đi sâu vào app."""

    host: str
    port: int
    database: str
    username: str
    password: str
    connection_type: str

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class RawTopicBinding:
    """Topic đầy đủ và ba phần được tách từ phải sang trái."""

    full_topic: str
    topic_prefix: str
    topic_schema: str
    table_name: str


@dataclass(frozen=True)
class TopicBinding:
    """Map Kafka topic về đúng Oracle owner/table dùng cho source SELECT."""

    full_topic: str
    topic_prefix: str
    topic_schema: str
    table: TableRef


@dataclass(frozen=True)
class GuardConfig:
    """Kết quả đã validate từ spGetKafkaGuardTopicConfig."""

    connector_name: str
    config_id: int
    credential: OracleCredential
    topics: tuple[RawTopicBinding, ...]
