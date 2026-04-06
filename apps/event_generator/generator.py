from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import count
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
CART_SEQUENCE = count(1)
ORDER_SEQUENCE = count(1)
FLOW_SEQUENCE = count(1)

DEVICE_TYPES = ("desktop", "mobile", "tablet")
PLATFORMS_BY_DEVICE = {
    "desktop": ("web",),
    "mobile": ("mobile_web", "ios_app", "android_app"),
    "tablet": ("mobile_web", "ios_app", "android_app"),
}
PAYMENT_METHODS = ("credit_card", "pix", "paypal", "voucher")
CHANNELS = ("direct", "paid_search", "paid_social", "email", "organic", "affiliate")
CHANNEL_WEIGHTS = {
    "new": (0.08, 0.28, 0.22, 0.14, 0.12, 0.16),
    "active": (0.18, 0.18, 0.12, 0.2, 0.2, 0.12),
    "vip": (0.28, 0.1, 0.06, 0.28, 0.18, 0.1),
    "churn_risk": (0.12, 0.22, 0.18, 0.16, 0.2, 0.12),
}
CHANNEL_REFERRERS = {
    "direct": ("",),
    "paid_search": ("google.com", "bing.com"),
    "paid_social": ("instagram.com", "facebook.com", "tiktok.com"),
    "email": ("newsletter", "crm_email"),
    "organic": ("google.com", "duckduckgo.com"),
    "affiliate": ("partner-site.com", "coupon-network.com"),
}
CHANNEL_SOURCES = {
    "direct": ("direct", "(none)"),
    "paid_search": ("google", "cpc"),
    "paid_social": ("meta", "paid_social"),
    "email": ("crm", "email"),
    "organic": ("google", "organic"),
    "affiliate": ("affiliate_network", "affiliate"),
}
RETURN_BY_CATEGORY = {
    "beauty": 0.04,
    "electronics": 0.08,
    "fashion": 0.17,
    "home": 0.06,
    "sports": 0.09,
}
DELIVERY_DAYS_BY_CATEGORY = {
    "beauty": (1, 4),
    "electronics": (2, 6),
    "fashion": (2, 5),
    "home": (3, 7),
    "sports": (2, 5),
}
SEARCH_TERMS_BY_CATEGORY = {
    "beauty": ("serum", "moisturizer", "gift set"),
    "electronics": ("wireless headphones", "monitor", "phone case"),
    "fashion": ("running shoes", "hoodie", "backpack"),
    "home": ("coffee maker", "desk lamp", "storage box"),
    "sports": ("yoga mat", "dumbbells", "trail shoes"),
}
PAGE_TYPES = ("home", "category", "search", "product")


@dataclass(frozen=True)
class EventRecord:
    event_type: str
    payload: dict[str, object]


def _scoped_id(prefix: str, sequence: count) -> str:
    return f"{prefix}-{INSTANCE_ID}-{next(sequence):06d}"


def _pick_customer() -> dict[str, object]:
    return random.choice(CUSTOMERS)


def _pick_product() -> dict[str, object]:
    return random.choice(ACTIVE_PRODUCTS)


def _pick_related_product(anchor: dict[str, object]) -> dict[str, object]:
    same_category = [product for product in ACTIVE_PRODUCTS if product is not anchor and product["category"] == anchor["category"]]
    same_brand = [product for product in ACTIVE_PRODUCTS if product is not anchor and product["brand"] == anchor["brand"]]
    r = random.random()
    if r < 0.7 and same_category:
        return random.choice(same_category)
    if r < 0.9 and same_brand:
        return random.choice(same_brand)
    return _pick_product()


def _campaign_id_for_channel(channel: str) -> str | None:
    if channel not in {"paid_search", "paid_social", "email", "affiliate"}:
        return None
    return f"CMP-{channel[:3].upper()}-{random.randint(1000, 9999)}"


def _session_context(customer: dict[str, object]) -> dict[str, object]:
    segment = str(customer.get("segment", "active"))
    device_type = random.choices(DEVICE_TYPES, weights=(0.35, 0.5, 0.15), k=1)[0]
    platform = random.choice(PLATFORMS_BY_DEVICE[device_type])
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS.get(segment, CHANNEL_WEIGHTS["active"]), k=1)[0]
    source, medium = CHANNEL_SOURCES[channel]
    campaign_id = _campaign_id_for_channel(channel)
    referrer = random.choice(CHANNEL_REFERRERS[channel])
    return {
        "segment": segment,
        "device_type": device_type,
        "platform": platform,
        "channel": channel,
        "campaign_id": campaign_id,
        "referrer": referrer,
        "source": source,
        "medium": medium,
    }


def _base_payload(
    *,
    event_type: str,
    event_ts: datetime,
    session_id: str,
    customer_id: str | None,
    product_id: str | None,
    cart_id: str | None,
    order_id: str | None,
    campaign_id: str | None,
    device_type: str,
    platform: str,
    page_type: str | None,
    referrer: str,
    quantity: int | None = None,
    unit_price: float | None = None,
    currency: str = "USD",
    attributes: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "event_id": _scoped_id("EVT", EVENT_SEQUENCE),
        "event_ts": event_ts.isoformat(),
        "event_type": event_type,
        "session_id": session_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "cart_id": cart_id,
        "order_id": order_id,
        "campaign_id": campaign_id,
        "device_type": device_type,
        "platform": platform,
        "page_type": page_type,
        "referrer": referrer,
        "quantity": quantity,
        "unit_price": round(unit_price, 2) if unit_price is not None else None,
        "currency": currency,
        "attributes": attributes or {},
    }
    return payload


def _event(
    *,
    event_type: str,
    event_ts: datetime,
    session_id: str,
    session_ctx: dict[str, object],
    customer_id: str | None,
    product_id: str | None = None,
    cart_id: str | None = None,
    order_id: str | None = None,
    page_type: str | None = None,
    quantity: int | None = None,
    unit_price: float | None = None,
    attributes: dict[str, object] | None = None,
) -> EventRecord:
    payload = _base_payload(
        event_type=event_type,
        event_ts=event_ts,
        session_id=session_id,
        customer_id=customer_id,
        product_id=product_id,
        cart_id=cart_id,
        order_id=order_id,
        campaign_id=session_ctx.get("campaign_id"),
        device_type=str(session_ctx["device_type"]),
        platform=str(session_ctx["platform"]),
        page_type=page_type,
        referrer=str(session_ctx["referrer"]),
        quantity=quantity,
        unit_price=unit_price,
        attributes=attributes,
    )
    return EventRecord(event_type=event_type, payload=payload)


def _next_ts(current: datetime, *, min_seconds: int = 5, max_seconds: int = 90) -> datetime:
    return current + timedelta(seconds=random.randint(min_seconds, max_seconds))


def _conversion_probability(*, customer: dict[str, object], session_ctx: dict[str, object], anchor_product: dict[str, object]) -> float:
    segment = str(customer.get("segment", "active"))
    base_by_segment = {
        "new": 0.12,
        "active": 0.28,
        "vip": 0.42,
        "churn_risk": 0.15,
    }
    probability = base_by_segment.get(segment, 0.22)
    channel_adjustments = {
        "direct": 0.04,
        "email": 0.08,
        "paid_search": 0.02,
        "paid_social": -0.01,
        "organic": 0.01,
        "affiliate": 0.00,
    }
    probability += channel_adjustments.get(str(session_ctx["channel"]), 0.0)

    if session_ctx["device_type"] == "mobile":
        probability -= 0.03
    elif session_ctx["device_type"] == "desktop":
        probability += 0.02

    price = float(anchor_product["price"])
    if price > 80:
        probability -= 0.05
    elif price < 30:
        probability += 0.03
    return max(0.03, min(0.8, probability))


def _page_view_count(device_type: str) -> int:
    if device_type == "mobile":
        return random.randint(2, 4)
    if device_type == "tablet":
        return random.randint(3, 5)
    return random.randint(4, 7)


def _cart_item_count() -> int:
    return random.choices((1, 2, 3, 4), weights=(0.58, 0.23, 0.13, 0.06), k=1)[0]


def generate_flow(*, purchase_probability: float = 0.35) -> list[EventRecord]:
    customer = _pick_customer()
    anchor_product = _pick_product()
    session_id = _scoped_id("SES", SESSION_SEQUENCE)
    cart_id = _scoped_id("CRT", CART_SEQUENCE)
    order_id = _scoped_id("ORD", ORDER_SEQUENCE)
    flow_id = _scoped_id("FLW", FLOW_SEQUENCE)
    session_ctx = _session_context(customer)
    customer_id = str(customer["customer_id"])
    event_ts = datetime.now(UTC)
    records: list[EventRecord] = []

    conversion_probability = _conversion_probability(customer=customer, session_ctx=session_ctx, anchor_product=anchor_product)
    effective_purchase_probability = max(0.01, min(0.95, purchase_probability * 0.5 + conversion_probability * 0.5))
    should_purchase = random.random() < effective_purchase_probability

    records.append(
        _event(
            event_type="session_started",
            event_ts=event_ts,
            session_id=session_id,
            session_ctx=session_ctx,
            customer_id=customer_id,
            page_type="home",
            attributes={
                "flow_id": flow_id,
                "channel": session_ctx["channel"],
                "source": session_ctx["source"],
                "medium": session_ctx["medium"],
                "customer_segment_seed": session_ctx["segment"],
            },
        )
    )

    page_views = _page_view_count(str(session_ctx["device_type"]))
    viewed_product_ids: list[str] = []
    for idx in range(page_views):
        event_ts = _next_ts(event_ts)
        page_type = random.choices(PAGE_TYPES, weights=(0.25, 0.2, 0.2, 0.35), k=1)[0]
        page_attributes = {"flow_id": flow_id, "step_index": idx + 1}
        product_id: str | None = None

        if page_type == "search":
            search_query = random.choice(SEARCH_TERMS_BY_CATEGORY[str(anchor_product["category"])])
            page_attributes["search_query"] = search_query
        elif page_type == "category":
            page_attributes["category_name"] = str(anchor_product["category"])
        elif page_type == "product":
            product = anchor_product if not viewed_product_ids or random.random() < 0.55 else _pick_related_product(anchor_product)
            product_id = str(product["product_id"])
            viewed_product_ids.append(product_id)
        records.append(
            _event(
                event_type="page_view",
                event_ts=event_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                product_id=product_id,
                page_type=page_type,
                attributes=page_attributes,
            )
        )

        if page_type == "search":
            records.append(
                _event(
                    event_type="search_performed",
                    event_ts=_next_ts(event_ts, min_seconds=1, max_seconds=10),
                    session_id=session_id,
                    session_ctx=session_ctx,
                    customer_id=customer_id,
                    page_type="search",
                    attributes={
                        "flow_id": flow_id,
                        "query": page_attributes["search_query"],
                        "results_category": str(anchor_product["category"]),
                    },
                )
            )

    if not viewed_product_ids:
        event_ts = _next_ts(event_ts)
        viewed_product_ids.append(str(anchor_product["product_id"]))
        records.append(
            _event(
                event_type="product_view",
                event_ts=event_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                product_id=str(anchor_product["product_id"]),
                page_type="product",
                unit_price=float(anchor_product["price"]),
                attributes={
                    "flow_id": flow_id,
                    "category_name": str(anchor_product["category"]),
                    "brand": str(anchor_product["brand"]),
                },
            )
        )

    item_target = _cart_item_count() if should_purchase else random.choices((0, 1, 2), weights=(0.35, 0.45, 0.2), k=1)[0]
    chosen_products: list[dict[str, object]] = [anchor_product]
    while len(chosen_products) < max(item_target, 1):
        candidate = _pick_related_product(anchor_product)
        if candidate["product_id"] not in {product["product_id"] for product in chosen_products}:
            chosen_products.append(candidate)

    line_items: list[dict[str, object]] = []
    for product in chosen_products[:item_target]:
        quantity = random.randint(1, 3)
        unit_price = float(product["price"])
        line_items.append(
            {
                "product": product,
                "quantity": quantity,
                "unit_price": unit_price,
                "gross_amount": round(quantity * unit_price, 2),
            }
        )

    for line_item in line_items:
        product = line_item["product"]
        event_ts = _next_ts(event_ts)
        records.append(
            _event(
                event_type="product_view",
                event_ts=event_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                product_id=str(product["product_id"]),
                page_type="product",
                unit_price=float(line_item["unit_price"]),
                attributes={
                    "flow_id": flow_id,
                    "category_name": str(product["category"]),
                    "brand": str(product["brand"]),
                },
            )
        )
        event_ts = _next_ts(event_ts, min_seconds=2, max_seconds=20)
        records.append(
            _event(
                event_type="add_to_cart",
                event_ts=event_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                product_id=str(product["product_id"]),
                cart_id=cart_id,
                page_type="cart",
                quantity=int(line_item["quantity"]),
                unit_price=float(line_item["unit_price"]),
                attributes={
                    "flow_id": flow_id,
                    "cart_item_count": len(line_items),
                    "category_name": str(product["category"]),
                },
            )
        )

    if line_items and random.random() < 0.18:
        removed = random.choice(line_items)
        line_items.remove(removed)
        product = removed["product"]
        event_ts = _next_ts(event_ts, min_seconds=1, max_seconds=12)
        records.append(
            _event(
                event_type="remove_from_cart",
                event_ts=event_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                product_id=str(product["product_id"]),
                cart_id=cart_id,
                page_type="cart",
                quantity=int(removed["quantity"]),
                unit_price=float(removed["unit_price"]),
                attributes={
                    "flow_id": flow_id,
                    "reason": random.choice(("price_sensitivity", "comparison_shopping", "changed_mind")),
                },
            )
        )

    if not should_purchase or not line_items:
        return records

    cart_value = round(sum(float(item["gross_amount"]) for item in line_items), 2)
    abandonment_probability = 0.08 + max(0, len(line_items) - 1) * 0.05
    if cart_value > 220:
        abandonment_probability += 0.06
    if session_ctx["device_type"] == "mobile":
        abandonment_probability += 0.04
    if random.random() < abandonment_probability:
        return records

    event_ts = _next_ts(event_ts, min_seconds=5, max_seconds=25)
    records.append(
        _event(
            event_type="checkout_started",
            event_ts=event_ts,
            session_id=session_id,
            session_ctx=session_ctx,
            customer_id=customer_id,
            cart_id=cart_id,
            page_type="checkout",
            attributes={
                "flow_id": flow_id,
                "cart_value": cart_value,
                "distinct_items": len(line_items),
            },
        )
    )

    payment_method = random.choice(PAYMENT_METHODS)
    event_ts = _next_ts(event_ts, min_seconds=2, max_seconds=18)
    records.append(
        _event(
            event_type="payment_attempted",
            event_ts=event_ts,
            session_id=session_id,
            session_ctx=session_ctx,
            customer_id=customer_id,
            cart_id=cart_id,
            order_id=order_id,
            page_type="checkout",
            attributes={
                "flow_id": flow_id,
                "payment_method": payment_method,
                "cart_value": cart_value,
            },
        )
    )

    payment_failure_probability = 0.08
    if payment_method == "voucher":
        payment_failure_probability += 0.03
    if cart_value > 250:
        payment_failure_probability += 0.03
    payment_succeeded = random.random() >= payment_failure_probability

    event_ts = _next_ts(event_ts, min_seconds=1, max_seconds=8)
    payment_event_type = "payment_succeeded" if payment_succeeded else "payment_failed"
    records.append(
        _event(
            event_type=payment_event_type,
            event_ts=event_ts,
            session_id=session_id,
            session_ctx=session_ctx,
            customer_id=customer_id,
            cart_id=cart_id,
            order_id=order_id,
            page_type="checkout",
            attributes={
                "flow_id": flow_id,
                "payment_method": payment_method,
                "attempted_amount": cart_value,
            },
        )
    )

    if not payment_succeeded:
        return records

    discount_amount = round(cart_value * random.choice((0.0, 0.0, 0.05, 0.1)), 2)
    shipping_amount = round(0 if cart_value >= 120 else random.choice((5.99, 7.99, 9.99)), 2)
    net_before_tax = max(0.0, cart_value - discount_amount)
    tax_amount = round(net_before_tax * 0.08, 2)
    net_amount = round(net_before_tax + shipping_amount + tax_amount, 2)
    warehouse_id = f"WH-{random.randint(1, 4):03d}"

    event_ts = _next_ts(event_ts, min_seconds=1, max_seconds=6)
    records.append(
        _event(
            event_type="order_placed",
            event_ts=event_ts,
            session_id=session_id,
            session_ctx=session_ctx,
            customer_id=customer_id,
            cart_id=cart_id,
            order_id=order_id,
            page_type="checkout",
            attributes={
                "flow_id": flow_id,
                "payment_method": payment_method,
                "gross_amount": cart_value,
                "discount_amount": discount_amount,
                "shipping_amount": shipping_amount,
                "tax_amount": tax_amount,
                "net_amount": net_amount,
                "item_count": sum(int(item["quantity"]) for item in line_items),
                "distinct_skus": len(line_items),
                "channel": session_ctx["channel"],
                "source": session_ctx["source"],
                "medium": session_ctx["medium"],
                "warehouse_id": warehouse_id,
                "order_items": [
                    {
                        "product_id": str(item["product"]["product_id"]),
                        "sku": str(item["product"]["sku"]),
                        "quantity": int(item["quantity"]),
                        "unit_price": float(item["unit_price"]),
                        "line_amount": float(item["gross_amount"]),
                    }
                    for item in line_items
                ],
            },
        )
    )

    cancellation_probability = 0.04 if session_ctx["segment"] != "churn_risk" else 0.08
    if random.random() < cancellation_probability:
        event_ts = _next_ts(event_ts, min_seconds=60, max_seconds=1800)
        records.append(
            _event(
                event_type="order_cancelled",
                event_ts=event_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                order_id=order_id,
                page_type=None,
                attributes={
                    "flow_id": flow_id,
                    "reason": random.choice(("customer_request", "payment_review", "inventory_shortage")),
                    "refund_amount": net_amount,
                },
            )
        )
        return records

    event_ts = _next_ts(event_ts, min_seconds=600, max_seconds=7200)
    records.append(
        _event(
            event_type="shipment_created",
            event_ts=event_ts,
            session_id=session_id,
            session_ctx=session_ctx,
            customer_id=customer_id,
            order_id=order_id,
            page_type=None,
            attributes={
                "flow_id": flow_id,
                "warehouse_id": warehouse_id,
                "shipment_carrier": random.choice(("ups", "fedex", "dhl", "local_courier")),
            },
        )
    )

    lead_time_days = max(DELIVERY_DAYS_BY_CATEGORY[str(item["product"]["category"])][1] for item in line_items)
    delivery_min, delivery_max = DELIVERY_DAYS_BY_CATEGORY[str(anchor_product["category"])]
    delivered_ts = event_ts + timedelta(days=random.randint(delivery_min, delivery_max), hours=random.randint(1, 18))
    records.append(
        _event(
            event_type="shipment_delivered",
            event_ts=delivered_ts,
            session_id=session_id,
            session_ctx=session_ctx,
            customer_id=customer_id,
            order_id=order_id,
            page_type=None,
            attributes={
                "flow_id": flow_id,
                "warehouse_id": warehouse_id,
                "delivery_lead_time_hours": round((delivered_ts - event_ts).total_seconds() / 3600, 2),
                "promised_sla_days": lead_time_days,
            },
        )
    )

    return_probability = max(RETURN_BY_CATEGORY[str(anchor_product["category"])], 0.03)
    if random.random() < return_probability:
        request_ts = delivered_ts + timedelta(days=random.randint(1, 14), hours=random.randint(1, 12))
        records.append(
            _event(
                event_type="return_requested",
                event_ts=request_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                order_id=order_id,
                page_type=None,
                attributes={
                    "flow_id": flow_id,
                    "reason": random.choice(("damaged", "not_as_described", "size_issue", "changed_mind")),
                    "requested_amount": net_amount,
                },
            )
        )

        completed_ts = request_ts + timedelta(days=random.randint(2, 10))
        records.append(
            _event(
                event_type="return_completed",
                event_ts=completed_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                order_id=order_id,
                page_type=None,
                attributes={
                    "flow_id": flow_id,
                    "refund_amount": net_amount,
                    "return_resolution": random.choice(("refund", "store_credit")),
                },
            )
        )

    if random.random() < 0.06:
        ticket_ts = delivered_ts + timedelta(hours=random.randint(4, 72))
        records.append(
            _event(
                event_type="support_ticket_created",
                event_ts=ticket_ts,
                session_id=session_id,
                session_ctx=session_ctx,
                customer_id=customer_id,
                order_id=order_id,
                page_type="support",
                attributes={
                    "flow_id": flow_id,
                    "ticket_reason": random.choice(("delivery_delay", "product_issue", "refund_status", "billing_question")),
                    "priority": random.choice(("low", "medium", "high")),
                },
            )
        )

    return records
