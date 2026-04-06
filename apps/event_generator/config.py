from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    databricks_host: str
    databricks_token: str
    databricks_volume_path: str
    mode: str = "events"
    flows: int = 100
    interval_ms: int = 500
    purchase_probability: float = 0.35
