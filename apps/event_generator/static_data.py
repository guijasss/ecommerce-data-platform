from __future__ import annotations

import json
import random
from datetime import UTC, date, datetime
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
ROOT_DIR = PACKAGE_DIR.parent if (PACKAGE_DIR.parent / "reference-data").exists() else PACKAGE_DIR.parents[1]
REFERENCE_DATA_DIR = ROOT_DIR / "reference-data"

with (REFERENCE_DATA_DIR / "customers.json").open(encoding="utf-8") as customer_file:
    CUSTOMERS: list[dict[str, object]] = json.load(customer_file)

with (REFERENCE_DATA_DIR / "products.json").open(encoding="utf-8") as product_file:
    PRODUCTS: list[dict[str, object]] = json.load(product_file)

COUNTRY_LOCATIONS = {
    "US": ("Austin", "Texas"),
    "MX": ("Monterrey", "Nuevo Leon"),
    "AR": ("Cordoba", "Cordoba"),
    "CL": ("Santiago", "Santiago Metropolitan"),
    "BR": ("Campinas", "Sao Paulo"),
}
ACQUISITION_CHANNELS = ("paid_search", "paid_social", "email", "organic", "direct", "affiliate")
CAMPAIGN_CHANNELS = ("paid_search", "paid_social", "email", "affiliate", "organic", "direct")
WAREHOUSE_REGIONS = (
    ("WH-001", "North America East", "na_east"),
    ("WH-002", "North America West", "na_west"),
    ("WH-003", "Latin America South", "latam_south"),
    ("WH-004", "Brazil Southeast", "br_southeast"),
)


def _stable_number(value: str) -> int:
    return sum(ord(char) * (index + 1) for index, char in enumerate(value))


def generate_dim_customers() -> list[dict[str, object]]:
    customers: list[dict[str, object]] = []
    for customer in CUSTOMERS:
        customer_id = str(customer["customer_id"])
        city, state = COUNTRY_LOCATIONS.get(str(customer.get("country", "US")), ("Unknown", "Unknown"))
        seed = _stable_number(customer_id)
        customers.append(
            {
                "customer_id": customer_id,
                "first_name": str(customer.get("first_name", "")),
                "last_name": str(customer.get("last_name", "")),
                "email": str(customer.get("email", "")),
                "signup_ts": str(customer.get("created_at", "")),
                "birth_year": 1965 + seed % 35,
                "city": city,
                "state": state,
                "country": str(customer.get("country", "")),
                "acquisition_channel": ACQUISITION_CHANNELS[seed % len(ACQUISITION_CHANNELS)],
                "customer_segment_seed": str(customer.get("segment", "active")),
                "is_marketing_opt_in": bool(seed % 5 != 0),
            }
        )
    return customers


def generate_dim_products() -> list[dict[str, object]]:
    products: list[dict[str, object]] = []
    for product in PRODUCTS:
        product_id = str(product["product_id"])
        unit_price = float(product.get("price", 0.0))
        category_name = str(product.get("category", "unknown"))
        products.append(
            {
                "product_id": product_id,
                "sku": str(product.get("sku", "")),
                "product_name": str(product.get("name", "")),
                "brand": str(product.get("brand", "")),
                "category_id": f"CAT-{category_name[:3].upper()}",
                "category_name": category_name,
                "unit_price": round(unit_price, 2),
                "cost": round(unit_price * 0.58, 2),
                "is_active": bool(product.get("active", False)),
                "created_ts": str(product.get("created_at", "")),
            }
        )
    return products


def generate_dim_campaigns(as_of: date | None = None) -> list[dict[str, object]]:
    anchor_date = as_of or datetime.now(UTC).date()
    campaigns: list[dict[str, object]] = []
    for index, channel in enumerate(CAMPAIGN_CHANNELS, start=1):
        start_date = anchor_date.replace(day=max(1, min(anchor_date.day, 20)))
        campaigns.append(
            {
                "campaign_id": f"CMP-{index:03d}",
                "channel": channel,
                "source": {
                    "paid_search": "google",
                    "paid_social": "meta",
                    "email": "crm",
                    "affiliate": "impact",
                    "organic": "seo",
                    "direct": "brand",
                }[channel],
                "medium": {
                    "paid_search": "cpc",
                    "paid_social": "paid_social",
                    "email": "email",
                    "affiliate": "affiliate",
                    "organic": "organic",
                    "direct": "(none)",
                }[channel],
                "campaign_name": f"{channel.replace('_', ' ').title()} Push {anchor_date.strftime('%Y-%m')}",
                "start_date": start_date.isoformat(),
                "end_date": anchor_date.isoformat(),
                "daily_budget": round(500 + index * 275.0, 2),
            }
        )
    return campaigns


def generate_dim_warehouses() -> list[dict[str, object]]:
    return [
        {"warehouse_id": warehouse_id, "warehouse_name": warehouse_name, "region": region}
        for warehouse_id, warehouse_name, region in WAREHOUSE_REGIONS
    ]


def generate_inventory_snapshot(snapshot_ts: str | None = None) -> list[dict[str, object]]:
    effective_ts = snapshot_ts or datetime.now(UTC).replace(microsecond=0).isoformat()
    warehouses = generate_dim_warehouses()
    products = generate_dim_products()
    inventory: list[dict[str, object]] = []
    for warehouse in warehouses:
        for product in products:
            seed = _stable_number(f"{warehouse['warehouse_id']}::{product['product_id']}")
            rng = random.Random(seed)
            on_hand_qty = rng.randint(0, 250)
            reserved_qty = rng.randint(0, min(40, on_hand_qty))
            inventory.append(
                {
                    "snapshot_ts": effective_ts,
                    "warehouse_id": warehouse["warehouse_id"],
                    "product_id": product["product_id"],
                    "on_hand_qty": on_hand_qty,
                    "reserved_qty": reserved_qty,
                    "available_qty": on_hand_qty - reserved_qty,
                }
            )
    return inventory
