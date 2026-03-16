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

DEVICE_TYPES = ("desktop", "mobile", "tablet")
LOCALES = ("en-US", "es-MX", "pt-BR")
CHANNELS = ("direct", "paid_search", "paid_social", "email", "organic_search", "referral")
PAYMENT_METHODS = ("credit_card", "pix", "paypal", "voucher")

ONBOARDING_STEPS = ("landing", "signup_start", "signup_complete", "profile", "preferences")

BROWSE_STEPS = (
    ("home", "landing"),
    ("search", "search_submit"),
    ("search", "search_result_click"),
    ("product", "view_product"),
    ("product", "recommendation_impression"),
    ("product", "recommendation_click"),
    ("cart", "add_to_cart"),
    ("checkout", "start_checkout"),
)
ABANDONMENT_STEPS = BROWSE_STEPS[:-2]


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
    customer: dict[str, object],
    session_ctx: dict[str, object],
    session_id: str,
    product_id: str | None,
    page: str,
    action: str,
    flow_id: str,
    journey_step: str | None = None,
    onboarding_step: str | None = None,
    event_position: int | None = None,
    component_id: str | None = None,
    component_type: str | None = None,
    search_query: str | None = None,
    search_filters: dict[str, object] | None = None,
    sort_order: str | None = None,
    recommendation_ctx: dict[str, object] | None = None,
    perf_ctx: dict[str, object] | None = None,
) -> dict[str, object]:
    event = _base_event("click")
    event.update(
        {
            "customer_id": str(customer["customer_id"]),
            "customer_country": str(customer.get("country", "")),
            "customer_segment": str(customer.get("segment", "")),
            "session_id": session_id,
            "product_id": product_id,
            "page": page,
            "action": action,
            "flow_id": flow_id,
            # Legacy field kept for compatibility with early examples.
            "source": str(session_ctx["channel"]),
            **session_ctx,
        }
    )

    if journey_step is not None:
        event["journey_step"] = journey_step
    if onboarding_step is not None:
        event["onboarding_step"] = onboarding_step
    if event_position is not None:
        event["event_position"] = event_position
    if component_id is not None:
        event["component_id"] = component_id
    if component_type is not None:
        event["component_type"] = component_type
    if search_query is not None:
        event["search_query"] = search_query
    if search_filters is not None:
        event["search_filters"] = search_filters
    if sort_order is not None:
        event["sort_order"] = sort_order
    if recommendation_ctx is not None:
        event.update(recommendation_ctx)
    if perf_ctx is not None:
        event.update(perf_ctx)
    return event


def _build_purchase_event(
    *,
    customer: dict[str, object],
    session_ctx: dict[str, object],
    session_id: str,
    order_id: str,
    product: dict[str, object],
    quantity: int,
    amount: float,
    currency: str,
    flow_id: str,
    unit_price: float,
    discount_amount: float = 0.0,
    discount_pct: float | None = None,
    coupon_code: str | None = None,
    order_total_amount: float | None = None,
    order_item_count: int | None = None,
    order_item_index: int | None = None,
    is_first_purchase: bool | None = None,
    cross_sell_source: str | None = None,
) -> dict[str, object]:
    event = _base_event("purchase")
    purchase_ctx = {
        "channel": session_ctx.get("channel", ""),
        "campaign_id": session_ctx.get("campaign_id", ""),
        "source_medium": session_ctx.get("source_medium", ""),
        "device_type": session_ctx.get("device_type", ""),
    }
    event.update(
        {
            "customer_id": str(customer["customer_id"]),
            "customer_country": str(customer.get("country", "")),
            "customer_segment": str(customer.get("segment", "")),
            "session_id": session_id,
            "order_id": order_id,
            "product_id": str(product["product_id"]),
            "product_sku": str(product.get("sku", "")),
            "product_category": str(product.get("category", "")),
            "product_brand": str(product.get("brand", "")),
            "quantity": quantity,
            "unit_price": unit_price,
            "amount": amount,
            "currency": currency,
            "flow_id": flow_id,
            **purchase_ctx,
        }
    )

    if discount_amount:
        event["discount_amount"] = round(discount_amount, 2)
    if discount_pct is not None:
        event["discount_pct"] = round(discount_pct, 2)
    if coupon_code is not None:
        event["coupon_code"] = coupon_code
    if order_total_amount is not None:
        event["order_total_amount"] = round(order_total_amount, 2)
    if order_item_count is not None:
        event["order_item_count"] = order_item_count
    if order_item_index is not None:
        event["order_item_index"] = order_item_index
    if is_first_purchase is not None:
        event["is_first_purchase"] = is_first_purchase
    if cross_sell_source is not None:
        event["cross_sell_source"] = cross_sell_source
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
            "payment_method": random.choice(PAYMENT_METHODS),
            "status": random.choice(["authorized", "settled"]),
            "amount": amount,
            "flow_id": flow_id,
        }
    )
    return event


def _pick_related_products(*, anchor: dict[str, object], count_items: int) -> list[dict[str, object]]:
    if count_items <= 1:
        return [anchor]

    anchor_category = str(anchor.get("category", ""))
    anchor_brand = str(anchor.get("brand", ""))
    by_category = [p for p in ACTIVE_PRODUCTS if str(p.get("category", "")) == anchor_category and p is not anchor]
    by_brand = [p for p in ACTIVE_PRODUCTS if str(p.get("brand", "")) == anchor_brand and p is not anchor]

    picked = [anchor]
    seen = {str(anchor["product_id"])}
    while len(picked) < count_items:
        r = random.random()
        if r < 0.6 and by_category:
            candidate = random.choice(by_category)
        elif r < 0.9 and by_brand:
            candidate = random.choice(by_brand)
        else:
            candidate = _pick_product()

        pid = str(candidate["product_id"])
        if pid in seen:
            continue
        picked.append(candidate)
        seen.add(pid)
    return picked


def _session_context(*, customer: dict[str, object]) -> dict[str, object]:
    channel = random.choice(CHANNELS)
    if channel == "paid_search":
        source_medium = "google/cpc"
    elif channel == "paid_social":
        source_medium = "facebook/paid_social"
    elif channel == "email":
        source_medium = "email/newsletter"
    elif channel == "organic_search":
        source_medium = "google/organic"
    elif channel == "referral":
        source_medium = "partner/referral"
    else:
        source_medium = "direct/(none)"

    device_type = random.choice(DEVICE_TYPES)
    os_name = "ios" if device_type == "mobile" and random.random() < 0.5 else "android" if device_type == "mobile" else "windows"

    segment = str(customer.get("segment", ""))
    is_logged_in = segment in {"active", "vip"} or (segment == "new" and random.random() < 0.3)

    experiment_id = random.choice(["exp_checkout_v2", "exp_home_reco", "exp_search_ranking"])
    experiment_variant = random.choice(["control", "variant_a", "variant_b"])

    campaign_id = f"cmp_{random.randint(1000, 9999)}" if channel in {"paid_search", "paid_social", "email"} else ""
    referrer = random.choice(["", "google.com", "instagram.com", "tiktok.com", "newsletter", "partner-site.com"])

    return {
        "channel": channel,
        "campaign_id": campaign_id,
        "source_medium": source_medium,
        "referrer": referrer,
        "device_type": device_type,
        "os": os_name,
        "app_version": random.choice(["1.12.0", "1.13.1", "1.14.0"]),
        "locale": random.choice(LOCALES),
        "is_logged_in": is_logged_in,
        "experiment_id": experiment_id,
        "experiment_variant": experiment_variant,
    }


def _perf_context(*, elapsed_ms: int) -> dict[str, object]:
    # Keep values plausible and cheap to compute; they are used for segmentation/UX analysis.
    return {
        "page_load_time_ms": random.randint(200, 3500),
        "component_load_time_ms": random.randint(20, 900),
        "time_on_previous_page_ms": random.randint(200, 20000),
        "session_elapsed_ms": elapsed_ms,
        "scroll_depth_pct": random.randint(0, 100),
        "product_impression_count": random.randint(0, 40),
    }


def _recommendation_context(*, seed_product_id: str | None = None) -> dict[str, object]:
    model = random.choice(["item2item", "co_purchase_graph", "similar_customers", "trending"])
    source = random.choice(["home_widget", "pdp_carousel", "cart_cross_sell"])
    request_id = _scoped_id("REC", EVENT_SEQUENCE)
    rank = random.randint(1, 10)
    return {
        "recommendation_model": model,
        "recommendation_source": source,
        "recommendation_request_id": request_id,
        "recommendation_rank": rank,
        "seed_product_id": seed_product_id,
    }


def generate_flow(*, purchase_probability: float = 0.35) -> list[EventRecord]:
    customer = _pick_customer()
    anchor_product = _pick_product()
    session_id = _scoped_id("SES", SESSION_SEQUENCE)
    flow_id = _scoped_id("FLW", FLOW_SEQUENCE)
    session_ctx = _session_context(customer=customer)
    should_purchase = random.random() < purchase_probability
    steps = BROWSE_STEPS if should_purchase else ABANDONMENT_STEPS[: random.randint(1, len(ABANDONMENT_STEPS))]

    elapsed_ms = 0
    records: list[EventRecord] = []

    # Optional onboarding funnel to support lifecycle modeling and similarity features.
    if str(customer.get("segment")) == "new" and random.random() < 0.8:
        for idx, step in enumerate(ONBOARDING_STEPS, start=1):
            elapsed_ms += random.randint(2000, 15000)
            records.append(
                EventRecord(
                    event_type="click",
                    payload=_build_click_event(
                        customer=customer,
                        session_ctx=session_ctx,
                        session_id=session_id,
                        product_id=None,
                        page="onboarding",
                        action=step,
                        flow_id=flow_id,
                        journey_step="onboarding",
                        onboarding_step=step,
                        event_position=idx,
                        component_id="onboarding_form",
                        component_type="form",
                        perf_ctx=_perf_context(elapsed_ms=elapsed_ms),
                    ),
                )
            )

    # Browsing funnel with search and recommendation interactions.
    search_query = random.choice(["wireless headphones", "running shoes", "coffee maker", "phone case", "backpack"])
    sort_order = random.choice(["relevance", "price_asc", "price_desc", "rating_desc"])
    anchor_category = str(anchor_product.get("category", ""))
    anchor_brand = str(anchor_product.get("brand", ""))
    search_filters = {"category": anchor_category, "brand": anchor_brand}

    for idx, (page, action) in enumerate(steps, start=1):
        elapsed_ms += random.randint(800, 12000)
        component_type = "page" if action in {"landing", "view_product"} else "button"
        component_id = f"{page}_{action}"

        product_id: str | None
        reco_ctx: dict[str, object] | None = None
        if page in {"product", "cart"}:
            product_id = str(anchor_product["product_id"])
        elif action in {"search_result_click"}:
            product_id = str(anchor_product["product_id"])
        elif action in {"recommendation_impression", "recommendation_click"}:
            product_id = str(_pick_product()["product_id"])
            reco_ctx = _recommendation_context(seed_product_id=str(anchor_product["product_id"]))
        else:
            product_id = None

        records.append(
            EventRecord(
                event_type="click",
                payload=_build_click_event(
                    customer=customer,
                    session_ctx=session_ctx,
                    session_id=session_id,
                    product_id=product_id,
                    page=page,
                    action=action,
                    flow_id=flow_id,
                    journey_step="browse",
                    event_position=idx,
                    component_id=component_id,
                    component_type=component_type,
                    search_query=search_query if page == "search" else None,
                    search_filters=search_filters if page == "search" else None,
                    sort_order=sort_order if page == "search" else None,
                    recommendation_ctx=reco_ctx,
                    perf_ctx=_perf_context(elapsed_ms=elapsed_ms),
                ),
            )
        )

    if not should_purchase:
        return records

    # Multi-item orders to support co-purchase and basket analysis.
    item_count = random.choices([1, 2, 3, 4], weights=[0.55, 0.25, 0.15, 0.05], k=1)[0]
    products = _pick_related_products(anchor=anchor_product, count_items=item_count)
    order_id = _scoped_id("ORD", ORDER_SEQUENCE)

    has_coupon = random.random() < 0.25
    coupon_code = random.choice(["WELCOME10", "VIP15", "BUNDLE5"]) if has_coupon else None
    discount_pct = random.choice([5.0, 10.0, 15.0, 20.0]) if has_coupon else 0.0

    line_items: list[tuple[dict[str, object], int, float, float]] = []
    for product in products:
        qty = random.randint(1, 3)
        unit_price = float(product["price"])
        line_amount = round(qty * unit_price, 2)
        line_discount = round(line_amount * (discount_pct / 100.0), 2) if discount_pct else 0.0
        line_items.append((product, qty, unit_price, line_discount))

    order_total_amount = round(sum((round(q * up, 2) - disc) for _, q, up, disc in line_items), 2)

    is_first_purchase = str(customer.get("segment")) == "new"

    for item_index, (product, qty, unit_price, line_discount) in enumerate(line_items, start=1):
        cross_sell_source = None
        if item_index > 1:
            cross_sell_source = random.choice(["frequently_bought_together", "similar_customers", "cart_cross_sell"])

        records.append(
            EventRecord(
                event_type="purchase",
                payload=_build_purchase_event(
                    customer=customer,
                    session_ctx=session_ctx,
                    session_id=session_id,
                    order_id=order_id,
                    product=product,
                    quantity=qty,
                    unit_price=unit_price,
                    amount=round(qty * unit_price - line_discount, 2),
                    discount_amount=line_discount,
                    discount_pct=discount_pct if has_coupon else None,
                    coupon_code=coupon_code,
                    currency="USD",
                    order_total_amount=order_total_amount,
                    order_item_count=len(line_items),
                    order_item_index=item_index,
                    is_first_purchase=is_first_purchase,
                    cross_sell_source=cross_sell_source,
                    flow_id=flow_id,
                ),
            )
        )
    records.append(
        EventRecord(
            event_type="transaction",
            payload=_build_transaction_event(
                customer_id=str(customer["customer_id"]),
                session_id=session_id,
                order_id=order_id,
                amount=order_total_amount,
                flow_id=flow_id,
            ),
        )
    )
    return records
