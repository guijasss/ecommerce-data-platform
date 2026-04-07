"""Synthetic ecommerce data generators."""

from apps.event_generator.generator import EventRecord, generate_flow
from apps.event_generator.static_data import (
    generate_dim_campaigns,
    generate_dim_customers,
    generate_dim_products,
    generate_dim_warehouses,
    generate_inventory_snapshot,
)

__all__ = [
    "EventRecord",
    "generate_dim_campaigns",
    "generate_dim_customers",
    "generate_dim_products",
    "generate_dim_warehouses",
    "generate_flow",
    "generate_inventory_snapshot",
]
