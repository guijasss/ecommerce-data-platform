from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Settings:
    target: Literal["local", "aiven"] = "local"
    auth_mode: Literal["plaintext", "ssl", "sasl_ssl"] = "plaintext"
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "ecommerce"
    flows: int = 100
    interval_ms: int = 500
    purchase_probability: float = 0.35
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
