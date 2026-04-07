from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime
from itertools import count
from pathlib import Path

from apps.event_generator.config import Settings
from apps.event_generator.generator import generate_flow
from apps.event_generator.publisher import DatabricksVolumePublisher
from dotenv import load_dotenv


def _load_dotenv() -> None:
    load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)


def parse_args() -> Settings:
    """Parse CLI arguments and map them to runtime settings."""
    _load_dotenv()

    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic ecommerce event flows that reference static JSON master data "
            "from the reference-data directory."
        ),
        epilog=(
            "Examples:\n"
            "  python3 -m apps.event_generator.main --flows 5 "
            "--debug-output-file /tmp/events.json\n"
            "  python3 -m apps.event_generator.main --flows 10 "
            "--databricks-host https://adb-... --databricks-token <token> "
            "--databricks-volume-path /Volumes/demo/ecommerce/raw"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--flows",
        type=int,
        default=0,
        help=(
            "Number of event flows to emit. "
            "Use 0 to run continuously until interrupted."
        ),
    )
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=500,
        help="Delay in milliseconds between event flows.",
    )
    parser.add_argument(
        "--purchase-probability",
        type=float,
        default=0.35,
        help=(
            "Base probability that a generated browsing session will become a purchase. "
            "Must be between 0.0 and 1.0. The generator combines this with customer, device, and product context."
        ),
    )
    parser.add_argument(
        "--debug-output-file",
        help=(
            "Write generated output to a local formatted JSON file instead of requiring a Databricks target. "
            "This requires a finite --flows value because continuous event generation "
            "cannot be serialized to a single local file."
        ),
    )

    # Databricks volume args (optional; env vars also supported).
    parser.add_argument(
        "--databricks-host",
        default=os.getenv("DATABRICKS_HOST"),
        help=(
            "Databricks workspace host, for example https://adb-1234567890123456.7.azuredatabricks.net. "
            "Defaults to DATABRICKS_HOST, including values loaded from .env."
        ),
    )
    parser.add_argument(
        "--databricks-token",
        default=os.getenv("DATABRICKS_TOKEN"),
        help="Databricks personal access token. Defaults to DATABRICKS_TOKEN, including values loaded from .env.",
    )
    parser.add_argument(
        "--databricks-volume-path",
        default=os.getenv("DATABRICKS_VOLUME_PATH"),
        help=(
            "Destination Unity Catalog Volume path, for example /Volumes/demo/ecommerce/raw. "
            "Defaults to DATABRICKS_VOLUME_PATH, including values loaded from .env."
        ),
    )

    args = parser.parse_args()

    if args.flows < 0:
        raise ValueError("--flows must be greater than or equal to 0.")

    if not 0.0 <= args.purchase_probability <= 1.0:
        raise ValueError("--purchase-probability must be between 0.0 and 1.0.")

    if args.debug_output_file and args.flows == 0:
        raise ValueError(
            "--debug-output-file requires a finite --flows value because "
            "continuous event generation cannot be serialized to a single local file."
        )

    has_databricks_target = bool(args.databricks_host and args.databricks_token and args.databricks_volume_path)
    if not has_databricks_target and not args.debug_output_file:
        raise ValueError(
            "Either provide --debug-output-file for local testing, or set "
            "--databricks-host, --databricks-token, and --databricks-volume-path "
            "(or the corresponding DATABRICKS_* environment variables, including values from .env)."
        )

    return Settings(
        flows=args.flows,
        interval_ms=args.interval_ms,
        purchase_probability=args.purchase_probability,
        databricks_host=args.databricks_host,
        databricks_token=args.databricks_token,
        databricks_volume_path=args.databricks_volume_path,
        debug_output_file=args.debug_output_file,
    )


def _write_debug_output(*, output_file: str, payload: object) -> None:
    path = Path(output_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"wrote debug payloads to {path}")


def main() -> None:
    settings = parse_args()

    publisher: DatabricksVolumePublisher | None = None
    if settings.databricks_host and settings.databricks_token and settings.databricks_volume_path:
        publisher = DatabricksVolumePublisher(
            host=settings.databricks_host,
            token=settings.databricks_token,
            volume_path=settings.databricks_volume_path,
        )

    total_label = "continuous" if settings.flows == 0 else str(settings.flows)
    debug_flows: list[dict[str, object]] = []

    for index in count(1):
        flow = generate_flow(purchase_probability=settings.purchase_probability)
        if settings.debug_output_file:
            debug_flows.append(
                {
                    "flow_index": index,
                    "event_count": len(flow),
                    "events": [record.payload for record in flow],
                }
            )
        if publisher is not None:
            for record in flow:
                publisher.publish(event_type=record.event_type, payload=record.payload)
            publisher.flush()
            print(f"[{index}/{total_label}] published flow with {len(flow)} events to {settings.databricks_volume_path}")
        else:
            print(f"[{index}/{total_label}] generated flow with {len(flow)} events")
        if settings.flows and index >= settings.flows:
            break
        time.sleep(settings.interval_ms / 1000)

    if settings.debug_output_file:
        _write_debug_output(
            output_file=settings.debug_output_file,
            payload={
                "generated_at": datetime.now(UTC).isoformat(),
                "flows_requested": settings.flows,
                "purchase_probability": settings.purchase_probability,
                "flows": debug_flows,
            },
        )


if __name__ == "__main__":
    main()
