from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    flows: int = 100
    interval_ms: int = 500
    purchase_probability: float = 0.35

    # Databricks Unity Catalog volume write target (Files API).
    databricks_host: str
    databricks_token: str
    databricks_volume_path: str
