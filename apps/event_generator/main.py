from __future__ import annotations

import argparse
import os
import time
from itertools import count

from .config import Settings
from .generator import generate_flow
from .publisher import DatabricksVolumePublisher


def parse_args() -> Settings:
    parser = argparse.ArgumentParser(description="Synthetic ecommerce event generator")
    parser.add_argument("--flows", type=int, default=0, help="Number of flows to emit. Use 0 to run continuously.")
    parser.add_argument("--interval-ms", type=int, default=500)
    parser.add_argument("--purchase-probability", type=float, default=0.35)

    # Databricks volume args (optional; env vars also supported).
    parser.add_argument("--databricks-host", default=os.getenv("DATABRICKS_HOST"))
    parser.add_argument("--databricks-token", default=os.getenv("DATABRICKS_TOKEN"))
    parser.add_argument("--databricks-volume-path", default=os.getenv("DATABRICKS_VOLUME_PATH"))

    args = parser.parse_args()

    if not args.databricks_host or not args.databricks_token or not args.databricks_volume_path:
        raise ValueError("DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_VOLUME_PATH are required.")

    return Settings(
        flows=args.flows,
        interval_ms=args.interval_ms,
        purchase_probability=args.purchase_probability,
        databricks_host=args.databricks_host,
        databricks_token=args.databricks_token,
        databricks_volume_path=args.databricks_volume_path,
    )


def main() -> None:
    settings = parse_args()

    publisher = DatabricksVolumePublisher(
        host=settings.databricks_host,
        token=settings.databricks_token,
        volume_path=settings.databricks_volume_path,
    )

    total_label = "continuous" if settings.flows == 0 else str(settings.flows)

    for index in count(1):
        flow = generate_flow(purchase_probability=settings.purchase_probability)
        for record in flow:
            publisher.publish(event_type=record.event_type, payload=record.payload)
        publisher.flush()
        print(f"[{index}/{total_label}] published flow with {len(flow)} events to {settings.databricks_volume_path}")
        if settings.flows and index >= settings.flows:
            break
        time.sleep(settings.interval_ms / 1000)


if __name__ == "__main__":
    main()
