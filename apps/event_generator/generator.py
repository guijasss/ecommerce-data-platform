from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from itertools import count
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

PACKAGE_DIR = Path(__file__).resolve().parent
ROOT_DIR = PACKAGE_DIR.parent if (PACKAGE_DIR.parent / "reference-data").exists() else PACKAGE_DIR.parents[1]
REFERENCE_DATA_DIR = ROOT_DIR / "reference-data"
INSTANCE_ID = os.getenv("EVENT_GENERATOR_INSTANCE_ID") or os.getenv("HOSTNAME") or uuid4().hex[:12]

with (REFERENCE_DATA_DIR / "customers.json").open(encoding="utf-8") as customer_file:
    CUSTOMERS: list[dict[str, object]] = json.load(customer_file)

with (REFERENCE_DATA_DIR / "products.json").open(encoding="utf-8") as product_file:
    PRODUCTS: list[dict[str, object]] = json.load(product_file)

ACTIVE_PRODUCTS = [product for product in PRODUCTS if product.get("active")]
EVENT_SEQUENCE = count(1)
SESSION_SEQUENCE = count(1)
ORDER_SEQUENCE = count(1)
TRANSACTION_SEQUENCE = count(1)
FLOW_SEQUENCE = count(1)

CLICK_STEPS = (
    ("home", "landing"),
    ("search", "browse_catalog"),
    ("product", "view_product"),
    ("cart", "add_to_cart"),
    ("checkout", "start_checkout"),
)
ABANDONMENT_STEPS = CLICK_STEPS[:-1]


@dataclass(frozen=True)
class EventRecord:
    event_type: str
    payload: dict[str, object]


def _scoped_id(prefix: str, sequence: count) -> str:
    return f"{prefix}-{INSTANCE_ID}-{next(sequence):06d}"


def _base_event(event_type: str) -> dict[str, object]:
    return {
        "event_id": _scoped_id("EVT", EVENT_SEQUENCE),
        "event_type": event_type,
        "occurred_at": datetime.now(UTC).isoformat(),
        "generator_instance_id": INSTANCE_ID,
    }


def _pick_customer() -> dict[str, object]:
    return random.choice(CUSTOMERS)


def _pick_product() -> dict[str, object]:
    return random.choice(ACTIVE_PRODUCTS)


def _build_click_event(
    *,
    customer_id: str,
    session_id: str,
    product_id: str,
    page: str,
    action: str,
    flow_id: str,
) -> dict[str, object]:
    event = _base_event("click")
    event.update(
        {
            "customer_id": customer_id,
            "session_id": session_id,
            "product_id": product_id,
            "page": page,
            "action": action,
            "flow_id": flow_id,
            "source": random.choice(["web", "mobile", "email"]),
        }
    )
    return event


def _build_purchase_event(
    *,
    customer_id: str,
    session_id: str,
    order_id: str,
    product_id: str,
    quantity: int,
    amount: float,
    currency: str,
    flow_id: str,
) -> dict[str, object]:
    event = _base_event("purchase")
    event.update(
        {
            "customer_id": customer_id,
            "session_id": session_id,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
            "amount": amount,
            "currency": currency,
            "flow_id": flow_id,
        }
    )
    return event


def _build_transaction_event(
    *,
    customer_id: str,
    session_id: str,
    order_id: str,
    amount: float,
    flow_id: str,
) -> dict[str, object]:
    event = _base_event("transaction")
    event.update(
        {
            "customer_id": customer_id,
            "session_id": session_id,
            "transaction_id": _scoped_id("TXN", TRANSACTION_SEQUENCE),
            "order_id": order_id,
            "payment_method": random.choice(["credit_card", "pix", "paypal", "voucher"]),
            "status": random.choice(["authorized", "settled"]),
            "amount": amount,
            "flow_id": flow_id,
        }
    )
    return event


def generate_flow(*, purchase_probability: float = 0.35) -> list[EventRecord]:
    customer = _pick_customer()
    product = _pick_product()
    customer_id = str(customer["customer_id"])
    product_id = str(product["product_id"])
    session_id = _scoped_id("SES", SESSION_SEQUENCE)
    flow_id = _scoped_id("FLW", FLOW_SEQUENCE)
    should_purchase = random.random() < purchase_probability
    steps = CLICK_STEPS if should_purchase else ABANDONMENT_STEPS[: random.randint(1, len(ABANDONMENT_STEPS))]

    records = [
        EventRecord(
            event_type="click",
            payload=_build_click_event(
                customer_id=customer_id,
                session_id=session_id,
                product_id=product_id,
                page=page,
                action=action,
                flow_id=flow_id,
            ),
        )
        for page, action in steps
    ]

    if not should_purchase:
        return records

    quantity = random.randint(1, 5)
    amount = round(quantity * float(product["price"]), 2)
    order_id = _scoped_id("ORD", ORDER_SEQUENCE)
    records.append(
        EventRecord(
            event_type="purchase",
            payload=_build_purchase_event(
                customer_id=customer_id,
                session_id=session_id,
                order_id=order_id,
                product_id=product_id,
                quantity=quantity,
                amount=amount,
                currency="USD",
                flow_id=flow_id,
            ),
        )
    )
    records.append(
        EventRecord(
            event_type="transaction",
            payload=_build_transaction_event(
                customer_id=customer_id,
                session_id=session_id,
                order_id=order_id,
                amount=amount,
                flow_id=flow_id,
            ),
        )
    )
    return records
