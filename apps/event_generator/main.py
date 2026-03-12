from __future__ import annotations

import argparse
import os
import time
from itertools import count

from .config import Settings
from .generator import generate_flow
from .publisher import EventPublisher


def resolve_connection_settings(
    args: argparse.Namespace,
) -> tuple[str, str, str, str | None, str | None, str | None, str | None, str | None, str | None]:
    if args.target == "aiven":
        bootstrap_servers = args.bootstrap_servers or os.getenv("AIVEN_KAFKA_BOOTSTRAP_SERVERS")
        if not bootstrap_servers:
            raise ValueError("AIVEN_KAFKA_BOOTSTRAP_SERVERS is required when --target aiven is used.")
        auth_mode = args.auth_mode or os.getenv("AIVEN_KAFKA_AUTH_MODE", "sasl_ssl")
        if auth_mode == "sasl_ssl":
            sasl_username = args.sasl_username or os.getenv("AIVEN_KAFKA_USERNAME")
            sasl_password = args.sasl_password or os.getenv("AIVEN_KAFKA_PASSWORD")
            ssl_cafile = args.ssl_cafile or os.getenv("AIVEN_KAFKA_SSL_CA")
            if not sasl_username or not sasl_password:
                raise ValueError(
                    "AIVEN_KAFKA_USERNAME and AIVEN_KAFKA_PASSWORD are required when auth mode is sasl_ssl."
                )
            return (
                bootstrap_servers,
                auth_mode,
                "SASL_SSL",
                args.sasl_mechanism or os.getenv("AIVEN_KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
                sasl_username,
                sasl_password,
                ssl_cafile,
                None,
                None,
            )
        return (
            bootstrap_servers,
            auth_mode,
            "SSL",
            None,
            None,
            None,
            args.ssl_cafile or os.getenv("AIVEN_KAFKA_SSL_CA"),
            args.ssl_certfile or os.getenv("AIVEN_KAFKA_SSL_CERT"),
            args.ssl_keyfile or os.getenv("AIVEN_KAFKA_SSL_KEY"),
        )
    return (args.bootstrap_servers or "localhost:9092", "plaintext", "PLAINTEXT", None, None, None, None, None, None)


def parse_args() -> Settings:
    parser = argparse.ArgumentParser(description="Synthetic ecommerce event generator")
    parser.add_argument("--target", choices=["local", "aiven"], default=os.getenv("EVENT_GENERATOR_TARGET", "local"))
    parser.add_argument(
        "--auth-mode",
        choices=["plaintext", "ssl", "sasl_ssl"],
        default=os.getenv("AIVEN_KAFKA_AUTH_MODE"),
        help="Kafka auth mode. For Aiven, defaults to sasl_ssl.",
    )
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    parser.add_argument("--topic-prefix", default="ecommerce")
    parser.add_argument("--flows", type=int, default=0, help="Number of flows to emit. Use 0 to run continuously.")
    parser.add_argument("--interval-ms", type=int, default=500)
    parser.add_argument("--purchase-probability", type=float, default=0.35)
    parser.add_argument("--sasl-mechanism", default=os.getenv("AIVEN_KAFKA_SASL_MECHANISM"))
    parser.add_argument("--sasl-username", default=os.getenv("AIVEN_KAFKA_USERNAME"))
    parser.add_argument("--sasl-password", default=os.getenv("AIVEN_KAFKA_PASSWORD"))
    parser.add_argument("--ssl-cafile", default=os.getenv("AIVEN_KAFKA_SSL_CA"))
    parser.add_argument("--ssl-certfile", default=os.getenv("AIVEN_KAFKA_SSL_CERT"))
    parser.add_argument("--ssl-keyfile", default=os.getenv("AIVEN_KAFKA_SSL_KEY"))
    args = parser.parse_args()
    (
        bootstrap_servers,
        auth_mode,
        security_protocol,
        sasl_mechanism,
        sasl_username,
        sasl_password,
        ssl_cafile,
        ssl_certfile,
        ssl_keyfile,
    ) = resolve_connection_settings(args)
    return Settings(
        target=args.target,
        auth_mode=auth_mode,
        bootstrap_servers=bootstrap_servers,
        topic_prefix=args.topic_prefix,
        flows=args.flows,
        interval_ms=args.interval_ms,
        purchase_probability=args.purchase_probability,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        ssl_cafile=ssl_cafile,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
    )


def main() -> None:
    settings = parse_args()
    publisher = EventPublisher(
        bootstrap_servers=settings.bootstrap_servers,
        topic_prefix=settings.topic_prefix,
        security_protocol=settings.security_protocol,
        sasl_mechanism=settings.sasl_mechanism,
        sasl_username=settings.sasl_username,
        sasl_password=settings.sasl_password,
        ssl_cafile=settings.ssl_cafile,
        ssl_certfile=settings.ssl_certfile,
        ssl_keyfile=settings.ssl_keyfile,
    )
    total_label = "continuous" if settings.flows == 0 else str(settings.flows)

    for index in count(1):
        flow = generate_flow(purchase_probability=settings.purchase_probability)
        for record in flow:
            publisher.publish(event_type=record.event_type, payload=record.payload)
        publisher.flush()
        print(
            f"[{index}/{total_label}] published flow with {len(flow)} events "
            f"to {settings.topic_prefix}.clicks/{settings.topic_prefix}.purchases/"
            f"{settings.topic_prefix}.transactions"
        )
        if settings.flows and index >= settings.flows:
            break
        time.sleep(settings.interval_ms / 1000)


if __name__ == "__main__":
    main()
