from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from kafka import KafkaProducer


@dataclass
class EventPublisher:
    bootstrap_servers: str
    topic_prefix: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None

    def __post_init__(self) -> None:
        producer_kwargs: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "value_serializer": lambda value: json.dumps(value).encode("utf-8"),
            "key_serializer": lambda value: value.encode("utf-8"),
            "security_protocol": self.security_protocol,
        }
        if self.security_protocol.upper() == "SASL_SSL":
            producer_kwargs.update(
                {
                    "sasl_mechanism": self.sasl_mechanism,
                    "sasl_plain_username": self.sasl_username,
                    "sasl_plain_password": self.sasl_password,
                    "ssl_cafile": self.ssl_cafile,
                }
            )
        if self.security_protocol.upper() == "SSL":
            producer_kwargs.update(
                {
                    "ssl_cafile": self.ssl_cafile,
                    "ssl_certfile": self.ssl_certfile,
                    "ssl_keyfile": self.ssl_keyfile,
                }
            )
        self._producer = KafkaProducer(**producer_kwargs)

    def publish(self, event_type: str, payload: dict[str, object]) -> None:
        topic = f"{self.topic_prefix}.{event_type}s"
        key = str(
            payload.get("session_id")
            or payload.get("order_id")
            or payload.get("transaction_id")
            or payload.get("event_id", event_type)
        )
        self._producer.send(topic, key=key, value=payload)

    def flush(self) -> None:
        self._producer.flush()
