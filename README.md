# Ecommerce Data Platform for Learning (Databricks Free Edition)

## 1. Project goal

Build a **learning-oriented ecommerce data platform** that simulates realistic customer behavior, processes data in **near real time**, and exposes business-ready analytical datasets using a **Databricks medallion architecture**.

The design below is constrained to **Databricks Free Edition**, which currently emphasizes **serverless compute** and is suitable for **incremental micro-batch processing** rather than always-on streaming. Because of that, the pipeline should use **file-based ingestion into a Unity Catalog Volume** and process arrivals with **Structured Streaming using `Trigger.AvailableNow`** instead of long-running infinite streams.

---

## 2. Project requirements / features

### Functional requirements

- Simulate **stateful ecommerce behavior** in near real time:
  - session start
  - page view / product view / search
  - add to cart / remove from cart
  - checkout start
  - payment success / failure
  - order placement
  - shipment / delivery / cancellation / return
- Include **static master data** and enforce interactions against it:
  - customers
  - products
  - categories
  - prices / promotions
  - warehouses / inventory
  - campaigns / channels
- Generate only the **minimum fields necessary for analytics**, not a fully operational OLTP schema.
- Persist generated raw files into a **Databricks Volume**.
- Process data through **Bronze → Silver → Gold** layers.
- Support graph-oriented analytics for:
  - customer clustering / community detection
  - products frequently bought together
- Use **open-source tools** and **Python**.

### Platform constraints

- Databricks Free Edition
- Serverless compute
- No permanently running streaming job
- Prefer **`Trigger.AvailableNow`** for ingestion / transformation micro-batches
- Prefer existing open-source generators where they fit; implement custom behavior only where necessary

---

## 3. Recommended technology stack

## Core platform
- **Databricks Free Edition**
- **Delta Lake** for bronze/silver/gold tables
- **Unity Catalog Volumes** for raw file landing and checkpoint-compatible storage paths
- **PySpark** for ingestion and transformations
- **Databricks Workflows / notebook scheduling** for repeated batch execution

## Data generation
- **dbldatagen**: synthetic Spark-native generation for large static datasets
- **Faker**: realistic names, addresses, emails, free-text attributes
- **Python custom state machine**: session and order event logic, because behavioral sequencing is domain-specific

## Graph analytics
- **NetworkX** as the default graph library for learning and prototyping in Python
- Optional: **Neo4j Community Edition** if you want a persistent graph database outside Databricks for exploration and Cypher-based analysis

## Developer tooling
- Python package structure with:
  - `src/generators/`
  - `src/pipelines/`
  - `src/metrics/`
  - `src/graphs/`
- `pytest` for tests
- `ruff` or `flake8` for linting
- `poetry` or `uv` for dependency management

---

## 4. Why this architecture fits Databricks Free Edition

Databricks documents the **medallion architecture** as the recommended layered design pattern for progressively improving data quality from raw to curated data. Databricks also documents **`Trigger.AvailableNow`** as a supported incremental processing pattern for Structured Streaming workloads, and Unity Catalog **Volumes** as the preferred object for governing non-tabular files. For this project, that means:

1. A generator writes raw event files every few minutes into a Volume.
2. A scheduled Databricks job reads all newly arrived files.
3. Bronze ingestion runs with **AvailableNow** and stops after consuming the backlog.
4. Silver and Gold jobs also run incrementally and terminate.
5. Re-running the schedule creates a near-real-time pattern without requiring an always-on stream.

This is the simplest design that respects Free Edition limits while still teaching modern lakehouse patterns.

---

## 5. High-level architecture

```text
Python Generators
  ├─ Static dimension generator (dbldatagen + Faker)
  └─ Stateful event generator (custom Python state machine)
                 |
                 v
      /Volumes/<catalog>/<schema>/<volume>/raw/
        ├─ customers/
        ├─ products/
        ├─ campaigns/
        ├─ inventory/
        └─ events/yyyy/mm/dd/hh/
                 |
                 v
        Bronze (raw append-only Delta tables)
                 |
                 v
        Silver (cleaned, deduplicated, conformed)
                 |
                 v
        Gold marts / feature tables
        ├─ sales
        ├─ marketing
        ├─ operations
        ├─ customer
        ├─ product
        └─ graph_features
```

---

## 6. Recommended ingestion pattern

### 6.1 Event generation cadence

Run a small Python generator every **1 to 5 minutes**.

Each execution should:
- sample active customers
- create sessions
- transition sessions through a realistic funnel
- emit one or more event files (prefer **JSON Lines** or compact **Parquet**)
- save files into a Volume path such as:

```text
/Volumes/demo/ecommerce/simulator/raw/events/dt=2026-04-06/hour=14/batch_000123.json
```

### 6.2 Processing cadence

Run Databricks jobs every **1 to 5 minutes** using **AvailableNow**:
- **Bronze job**: ingest newly landed files
- **Silver job**: cleanse and sessionize
- **Gold job**: refresh incremental marts

This gives a near-real-time experience while staying compatible with serverless execution.

---

## 7. Data model to generate

## 7.1 Static data (dimension-like)

### `dim_customer`
Minimal fields:
- `customer_id`
- `first_name`
- `last_name`
- `email`
- `signup_ts`
- `birth_year` or `age_band`
- `city`
- `state`
- `country`
- `acquisition_channel`
- `customer_segment_seed` (initial label for simulation)
- `is_marketing_opt_in`

### `dim_product`
Minimal fields:
- `product_id`
- `sku`
- `product_name`
- `brand`
- `category_id`
- `category_name`
- `unit_price`
- `cost`
- `is_active`
- `created_ts`

### `dim_campaign`
Minimal fields:
- `campaign_id`
- `channel` (paid_search, paid_social, email, organic, direct, affiliate)
- `source`
- `medium`
- `campaign_name`
- `start_date`
- `end_date`
- `daily_budget`

### `dim_warehouse`
Minimal fields:
- `warehouse_id`
- `warehouse_name`
- `region`

### `inventory_snapshot`
Minimal fields:
- `snapshot_ts`
- `warehouse_id`
- `product_id`
- `on_hand_qty`
- `reserved_qty`
- `available_qty`

---

## 7.2 Behavioral / transactional events

Use a single raw event contract in Bronze and normalize in Silver.

### `fact_event_raw`
Common fields:
- `event_id`
- `event_ts`
- `event_type`
- `session_id`
- `customer_id` (nullable for anonymous sessions if desired)
- `product_id` (nullable)
- `cart_id` (nullable)
- `order_id` (nullable)
- `campaign_id` (nullable)
- `device_type`
- `platform`
- `page_type`
- `referrer`
- `quantity`
- `unit_price`
- `currency`
- `attributes` (JSON map for event-specific extras)

### Event types to implement
Minimum useful set:
- `session_started`
- `page_view`
- `product_view`
- `search_performed`
- `add_to_cart`
- `remove_from_cart`
- `checkout_started`
- `payment_attempted`
- `payment_succeeded`
- `payment_failed`
- `order_placed`
- `order_cancelled`
- `shipment_created`
- `shipment_delivered`
- `return_requested`
- `return_completed`
- `support_ticket_created` *(optional but useful for support KPIs)*

---

## 7.3 Silver tables

### `silver_sessions`
One row per session:
- `session_id`
- `customer_id`
- `session_start_ts`
- `session_end_ts`
- `traffic_channel`
- `campaign_id`
- `device_type`
- `page_views`
- `product_views`
- `searches`
- `adds_to_cart`
- `removes_from_cart`
- `checkout_started_flag`
- `purchase_flag`
- `items_purchased`
- `gross_revenue`

### `silver_carts`
- `cart_id`
- `session_id`
- `customer_id`
- `cart_created_ts`
- `checkout_started_ts`
- `cart_abandoned_flag`
- `cart_value`
- `distinct_items`

### `silver_orders`
One row per order:
- `order_id`
- `customer_id`
- `session_id`
- `order_ts`
- `payment_status`
- `order_status`
- `gross_amount`
- `discount_amount`
- `net_amount`
- `shipping_amount`
- `tax_amount`
- `item_count`
- `distinct_skus`
- `campaign_id`
- `channel`
- `warehouse_id`
- `delivered_ts`
- `cancelled_ts`
- `returned_ts`

### `silver_order_items`
- `order_id`
- `order_item_id`
- `customer_id`
- `product_id`
- `quantity`
- `unit_price`
- `discount_amount`
- `line_amount`
- `cost_amount`
- `margin_amount`

### `silver_marketing_touchpoints`
- `session_id`
- `customer_id`
- `campaign_id`
- `channel`
- `impression_flag`
- `click_flag`
- `session_flag`
- `conversion_flag`
- `attributed_order_id`
- `attributed_revenue`

### `silver_fulfillment`
- `order_id`
- `warehouse_id`
- `shipment_created_ts`
- `shipment_delivered_ts`
- `delivery_lead_time_hours`
- `fulfilled_on_time_flag`
- `return_flag`

---

## 7.4 Gold marts

### `gold_sales_daily`
Daily sales KPIs by date, channel, category, brand.

### `gold_marketing_daily`
Campaign and channel performance KPIs.

### `gold_operations_daily`
Inventory, fulfillment, return, and delivery KPIs.

### `gold_customer_360`
Customer-level aggregates for segmentation, RFM, retention, and graph export.

### `gold_product_360`
Product performance, conversion, margin, and co-purchase features.

### `gold_graph_edges_customer_product`
Weighted edges between customers and products.

### `gold_graph_edges_product_product`
Weighted co-purchase edges between products.

### `gold_graph_edges_customer_customer`
Similarity edges based on category overlap, co-view, or co-purchase behavior.

---

## 8. Stateful event generation logic

Use existing tools where they help, but keep the behavioral sequence custom.

## 8.1 What to reuse

### Use `dbldatagen` for
- customers
- products
- campaigns
- warehouses
- inventory snapshots

### Use `Faker` for
- names
- emails
- addresses / cities
- campaign names
- product text placeholders if needed

## 8.2 What to implement from scratch

Implement a **customer/session state machine** because this is the part that generic generators do not model well.

### Recommended session state machine

```text
START
  -> session_started
  -> page_view (home/category/search/product)
  -> [0..N] product_view
  -> [0..N] add_to_cart
  -> [0..N] remove_from_cart
  -> [0..1] checkout_started
  -> [0..1] payment_attempted
      -> payment_failed -> END
      -> payment_succeeded -> order_placed
            -> [0..1] shipment_created
            -> [0..1] shipment_delivered
            -> [0..1] return_requested
            -> [0..1] return_completed
END
```

## 8.3 Behavior realism rules

To make analytics meaningful, encode probabilities by segment and context:
- new customers convert less than returning customers
- higher-priced products have lower conversion probability
- products in the same category are more likely to be viewed together
- mobile sessions have shorter depth than desktop sessions
- carts with more items have higher AOV but also higher abandonment risk
- delivery and return probabilities vary by category
- campaign channel influences both session arrival rate and conversion rate

## 8.4 Minimal simulation entities

Maintain in memory for each generator run:
- customer profile
- active session state
- active cart state
- product affinity matrix or category preferences
- campaign attribution for session

This is enough to produce coherent event sequences without building a full application backend.

---

## 9. Recommended file layout inside the Databricks Volume

```text
/Volumes/<catalog>/<schema>/<volume>/
  raw/
    customers/
    products/
    campaigns/
    warehouses/
    inventory/
    events/dt=YYYY-MM-DD/hour=HH/
  checkpoints/
    bronze_events/
    silver_sessions/
    silver_orders/
    gold_sales/
  exports/
    graph/
      customer_product/
      product_product/
      customer_customer/
```

Notes:
- Keep **raw** file-oriented.
- Keep **checkpoints** separate from raw and curated data.
- Write **Delta tables** to managed schemas; use the Volume primarily as the landing/export zone.

---

## 10. Medallion implementation details

## Bronze
Purpose:
- ingest raw files exactly as produced
- preserve source schema and payload
- add ingestion metadata

Typical enrichments:
- `ingest_ts`
- `source_file`
- `batch_id`
- `_rescued_data` or equivalent handling for malformed records

## Silver
Purpose:
- deduplicate events
- enforce valid foreign keys against product/customer masters
- normalize timestamps and currencies
- build sessions, carts, orders, order items, fulfillment facts
- infer abandonment and delivery lead time

Typical rules:
- drop duplicate `event_id`
- reject unknown `product_id` into quarantine table
- require correct state transitions (`order_placed` should follow successful payment)

## Gold
Purpose:
- expose business-friendly tables by domain
- aggregate at daily / weekly / monthly grains
- generate feature tables for graph and ML experiments

---

## 11. Analytical metrics by business area

The set below prioritizes metrics that are widely used across ecommerce analytics and that can be computed from the proposed generated data.

| Business area | Metric | Why it matters | Required generated data |
|---|---|---|---|
| Marketing | Sessions | Traffic baseline | session_started, campaign/channel |
| Marketing | Conversion rate | Efficiency of traffic turning into orders | sessions, orders |
| Marketing | CAC | Cost to acquire customers | campaign spend + first orders/new customers |
| Marketing | ROAS | Revenue per ad dollar | campaign spend + attributed revenue |
| Marketing | CTR | Ad/email engagement | impressions + clicks |
| Marketing | Revenue by channel/source/medium | Channel mix analysis | campaign attribution + orders |
| Marketing | New customer rate | Acquisition health | customer signup + first order |
| Sales | Gross merchandise value (GMV) | Top-line demand | orders/order_items |
| Sales | Net revenue | Realized sales after discounts/returns | orders + returns |
| Sales | Orders | Core sales volume | orders |
| Sales | Units sold | Product demand | order_items |
| Sales | Average order value (AOV) | Basket size | orders |
| Sales | Items per order | Basket composition | order_items |
| Sales | Product conversion rate | Product page effectiveness | product_view + order_items |
| Sales | Cart abandonment rate | Checkout leakage | carts + checkout_started + orders |
| Sales | Repeat purchase rate | Loyalty / retention | customer orders |
| Sales | Customer lifetime value (CLV/LTV) | Long-term customer value | customer order history |
| Product / Merchandising | Product views | Demand signal | product_view |
| Product / Merchandising | View-to-cart rate | Merchandising effectiveness | product_view + add_to_cart |
| Product / Merchandising | Cart-to-purchase rate | Funnel quality | add_to_cart + orders |
| Product / Merchandising | Category revenue | Assortment performance | products + order_items |
| Product / Merchandising | Gross margin by product/category | Profitability | product cost + order_items |
| Operations | Inventory available | Stock health | inventory_snapshot |
| Operations | Stockout rate | Lost-sales risk | inventory_snapshot + attempted adds/orders |
| Operations | Inventory turnover | Working capital efficiency | inventory + COGS |
| Operations | Order fulfillment rate | Operational reliability | orders + shipments |
| Operations | On-time delivery rate | Customer promise adherence | fulfillment timestamps + SLA target |
| Operations | Delivery lead time | Fulfillment speed | shipment_created + delivered |
| Operations | Cancellation rate | Demand quality / ops issues | orders + cancellations |
| Operations | Return rate | Product / fulfillment quality | orders + returns |
| Customer Service | Tickets per order | Support burden | support tickets + orders |
| Customer Service | First response time | Support speed | support tickets |
| Customer Service | CSAT (optional) | Customer satisfaction | support survey/ticket outcome |
| Finance | Discount rate | Margin pressure | order discounts |
| Finance | Gross margin | Profitability | price, cost, discounts |
| Finance | Refund amount | Revenue leakage | returns/refunds |
| Finance | Revenue after returns | Clean realized revenue | orders + returns |

### Minimum extra data needed to support these metrics

To calculate the table above, add these datasets/events beyond simple browsing and orders:
- campaign spend / impressions / clicks
- inventory snapshots
- product cost
- shipment milestones
- returns
- optional support tickets and CSAT events

---

## 12. Graph analytics design

## 12.1 Core graph models

### A. Customer-product bipartite graph
Nodes:
- `Customer`
- `Product`

Edges:
- `VIEWED`
- `ADDED_TO_CART`
- `PURCHASED`

Useful edge properties:
- `count`
- `last_ts`
- `revenue`
- `quantity`
- `channel`

### B. Product-product co-purchase graph
Nodes:
- `Product`

Edges:
- `BOUGHT_TOGETHER`

Edge weight examples:
- same-order count
- support / confidence / lift

### C. Customer-customer similarity graph
Nodes:
- `Customer`

Edges:
- `SIMILAR_TO`

Edge weight examples:
- overlapping categories purchased
- cosine similarity on product/category vectors
- shared campaign affinity

---

## 12.2 Your requested graph use cases

### Customer clustering
Recommended approach:
1. Build customer embeddings or feature vectors from category, brand, price, and purchase frequency behavior.
2. Create a similarity graph.
3. Run community detection (for example Louvain/Leiden where available) or connected-component style methods.

Potential outputs:
- bargain hunters
- premium shoppers
- category specialists
- seasonal customers
- churn-risk customers

### Frequently bought together
Recommended approach:
1. Build `product_product` edges from same-order co-occurrence.
2. Store pair support counts.
3. Calculate confidence / lift.
4. Rank neighbors per product.

This supports classic recommendation blocks such as “customers also bought”.

---

## 12.3 Additional graph use cases worth adding

### 1. Cross-sell recommendations
Use product-product or customer-product neighborhoods to suggest complementary items.

### 2. Customer journey path analysis
Model page or event transitions as a directed graph to find the most common and highest-drop-off paths.

### 3. Fraud / abuse detection
Detect suspicious clusters of customers, devices, payment methods, addresses, or unusually dense return/refund behavior.

### 4. Influence of acquisition channels
Build graphs linking campaigns, sessions, customers, and orders to see which channels create similar downstream purchase communities.

### 5. Inventory substitution opportunities
Connect products by similarity and stock status to recommend alternatives during stockouts.

### 6. Category affinity mapping
Identify categories that are often explored or purchased together to improve catalog layout and bundles.

### 7. Warehouse-to-product dependency graph
Model which warehouses are critical to specific categories or customer regions to simulate fulfillment bottlenecks.

### 8. Churn-risk neighborhood analysis
Customers connected to declining categories, low visit frequency, or high ticket/return communities can be flagged for retention actions.

---

## 13. Open-source implementation recommendation

## Best default choice
Use:
- **Databricks + Delta + PySpark** for the lakehouse
- **dbldatagen + Faker** for synthetic base data
- **custom Python state machine** for user behavior events
- **NetworkX** for graph algorithms inside Python notebooks/jobs

This is the cleanest setup for a learning project because it stays fully in Python and minimizes platform sprawl.

## Optional extension
If you want a true graph database experience:
- export graph edges/nodes from Gold to Parquet/CSV
- load them into **Neo4j Community Edition**
- run Cypher queries and graph algorithms externally

That adds complexity, so it should be a second phase, not phase one.

---

## 14. Suggested project phases

### Phase 1 — Foundation
- create catalog/schema/volume
- generate static dimensions
- generate basic page/cart/order events
- build bronze and silver

### Phase 2 — Analytical value
- build sales / marketing / operations gold marts
- add inventory, campaign, shipment, and return generation
- validate KPI calculations

### Phase 3 — Graph analytics
- export graph edge tables
- compute product co-purchase graph
- compute customer similarity graph
- test clustering and recommendations

### Phase 4 — Realism improvements
- add seasonality
- add promotion effects
- add category-specific return behavior
- add customer-service interactions

---

## 15. Practical recommendation on scope

For a first version, do **not** simulate everything.

Start with this minimal but high-value subset:
- customers
- products
- campaigns
- sessions
- product views
- add to cart
- checkout started
- payment succeeded / failed
- orders and order items
- inventory snapshots
- shipments and returns

That subset is already enough to compute most of the most useful ecommerce KPIs and enables graph experiments.

---

## 16. Final recommendation

The best design for your constraints is a **file-landed micro-batch ecommerce simulator**:

- use **dbldatagen** for static master data
- use **Faker** for realistic values
- implement **stateful customer/session logic** yourself
- write raw files into a **Databricks Volume**
- process incrementally with **`Trigger.AvailableNow`**
- expose domain-oriented **Gold marts** for marketing, sales, operations, customer, and product analytics
- build graph-ready edge tables and start with **NetworkX** before introducing a standalone graph database

This gives you a project that is realistic enough to teach modern data engineering patterns, but still small enough to complete on Databricks Free Edition.

---

## 17. References

- Databricks: medallion architecture
- Databricks: structured streaming trigger intervals / `AvailableNow`
- Databricks: serverless compute
- Databricks: Unity Catalog Volumes and file handling
- Databricks Labs: `dbldatagen`
- Faker documentation
- NetworkX documentation
- Neo4j Community Edition / open source licensing
- Shopify: ecommerce KPIs, conversion rate, AOV, CLV
- BigCommerce: ecommerce metrics and analytics reporting
- Mailchimp: ROAS, CAC, CTR
- Zendesk: support KPIs, first response time, CSAT
