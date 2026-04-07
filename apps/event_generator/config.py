from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    databricks_host: str | None
    databricks_token: str | None
    databricks_volume_path: str | None
    flows: int = 100
    interval_ms: int = 500
    purchase_probability: float = 0.35
    debug_output_file: str | None = None
