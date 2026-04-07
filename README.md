# Ecommerce Data Platform for Learning

## Project Goal

Build a learning-oriented ecommerce data platform that generates realistic event data, processes it in near real time with a medallion architecture, and exposes business-ready datasets for analytics and graph-based exploration.

The project is designed for Databricks Free Edition, so ingestion should be file-based and processed as recurring micro-batches instead of always-on streaming jobs.

## Requirements

- Support near-real-time ingestion of generated ecommerce events.
- Use file landing into a Unity Catalog Volume as the raw ingestion boundary.
- Process data through Bronze, Silver, and Gold layers.
- Use Structured Streaming with `Trigger.AvailableNow` or equivalent micro-batch execution compatible with serverless compute.
- Generate event flows for sessions, browsing, cart activity, checkout, payments, orders, shipment, delivery, cancellations, and returns.
- Keep static master data in JSON files for customers, products, campaigns, warehouses, and inventory.
- Ensure generated events reference valid IDs from the static master data.
- Produce curated datasets that support marketing, sales, operations, product, and finance analytics.
- Support graph-oriented analysis such as customer clustering, product co-purchase, and customer-product relationships.
- Stay primarily within open-source Python tooling where possible.

## Tech Stack

- Databricks Free Edition
- Delta Lake
- Unity Catalog Volumes
- PySpark
- Structured Streaming with `Trigger.AvailableNow`
- Python event generator
- JSON reference data files
- Faker for realistic synthetic attributes
- NetworkX for graph algorithms
- `pytest` for tests

## Analytical Metrics

### Marketing

- Sessions by channel
- Conversion rate
- Revenue by campaign

### Sales

- Gross merchandise value (GMV)
- Orders
- Average order value (AOV)

### Product / Merchandising

- Product views
- View-to-cart rate
- Product revenue

### Operations

- Inventory available
- Delivery lead time
- Return rate

### Finance

- Net revenue
- Discount rate
- Gross margin
