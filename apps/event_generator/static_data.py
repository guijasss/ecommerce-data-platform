from __future__ import annotations

import json
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
ROOT_DIR = PACKAGE_DIR.parent if (PACKAGE_DIR.parent / "reference-data").exists() else PACKAGE_DIR.parents[1]
REFERENCE_DATA_DIR = ROOT_DIR / "reference-data"

def _load_records(filename: str) -> list[dict[str, object]]:
    with (REFERENCE_DATA_DIR / filename).open(encoding="utf-8") as input_file:
        return json.load(input_file)


CUSTOMERS = _load_records("customers.json")
PRODUCTS = _load_records("products.json")
CAMPAIGNS = _load_records("campaigns.json")
WAREHOUSES = _load_records("warehouses.json")
INVENTORY_SNAPSHOT = _load_records("inventory.json")

COUNTRY_LOCATIONS = {
    "US": ("Austin", "Texas"),
    "MX": ("Monterrey", "Nuevo Leon"),
    "AR": ("Cordoba", "Cordoba"),
    "CL": ("Santiago", "Santiago Metropolitan"),
    "BR": ("Campinas", "Sao Paulo"),
}
ACQUISITION_CHANNELS = ("paid_search", "paid_social", "email", "organic", "direct", "affiliate")


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
        unit_price = float(product.get("price", 0.0))
        category_name = str(product.get("category", "unknown"))
        products.append(
            {
                "product_id": str(product["product_id"]),
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


def generate_dim_campaigns() -> list[dict[str, object]]:
    return [dict(campaign) for campaign in CAMPAIGNS]


def generate_dim_warehouses() -> list[dict[str, object]]:
    return [dict(warehouse) for warehouse in WAREHOUSES]


def generate_inventory_snapshot() -> list[dict[str, object]]:
    return [dict(record) for record in INVENTORY_SNAPSHOT]
