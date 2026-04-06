# Inventário de tabelas

## `main.silver.clicks`

### Schema

| coluna | tipo | nullable |
|---|---|---|
| action | string | True |
| app_version | string | True |
| campaign_id | string | True |
| channel | string | True |
| component_id | string | True |
| component_load_time_ms | int | True |
| component_type | string | True |
| customer_country | string | True |
| customer_id | string | True |
| customer_segment | string | True |
| device_type | string | True |
| event_id | string | True |
| event_position | string | True |
| event_type | string | True |
| experiment_id | string | True |
| experiment_variant | string | True |
| flow_id | string | True |
| generator_instance_id | string | True |
| is_logged_in | boolean | True |
| journey_step | string | True |
| locale | string | True |
| occurred_at | timestamp | True |
| onboarding_step | string | True |
| os | string | True |
| page | string | True |
| page_load_time_ms | int | True |
| product_id | string | True |
| referrer | string | True |
| scroll_depth_pct | string | True |
| search_filters | string | True |
| search_query | string | True |
| session_elapsed_ms | int | True |
| session_id | string | True |
| sort_order | string | True |
| source | string | True |
| source_medium | string | True |
| time_on_previous_page_ms | int | True |
| ingest_date | date | True |
| _rescued_data | string | True |

### Sample

| action | app_version | campaign_id | channel | component_id | component_load_time_ms | component_type | customer_country | customer_id | customer_segment | device_type | event_id | event_position | event_type | experiment_id | experiment_variant | flow_id | generator_instance_id | is_logged_in | journey_step | locale | occurred_at | onboarding_step | os | page | page_load_time_ms | product_id | referrer | scroll_depth_pct | search_filters | search_query | session_elapsed_ms | session_id | sort_order | source | source_medium | time_on_previous_page_ms | ingest_date | _rescued_data |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| landing | 1.12.0 | null | referral | home_landing | 728 | page | CL | CUST039 | churn_risk | mobile | EVT-773337819c78-000994 | 1 | click | exp_home_reco | variant_b | FLW-773337819c78-000135 | 773337819c78 | False | browse | es-MX | 2026-03-16 16:04:35.066532 | null | ios | home | 1328 | null | tiktok.com | 56 | null | null | 9796 | SES-773337819c78-000135 | null | referral | partner/referral | 2720 | 2026-03-16 | null |
| search_submit | 1.12.0 | null | referral | search_search_submit | 582 | button | CL | CUST039 | churn_risk | mobile | EVT-773337819c78-000995 | 2 | click | exp_home_reco | variant_b | FLW-773337819c78-000135 | 773337819c78 | False | browse | es-MX | 2026-03-16 16:04:35.066595 | null | ios | search | 318 | null | tiktok.com | 80 | {"category":"beauty","brand":"Atlas"} | coffee maker | 18523 | SES-773337819c78-000135 | price_asc | referral | partner/referral | 14412 | 2026-03-16 | null |
| landing | 1.13.1 | cmp_1352 | email | home_landing | 504 | page | CL | CUST049 | active | mobile | EVT-773337819c78-000601 | 1 | click | exp_search_ranking | variant_b | FLW-773337819c78-000081 | 773337819c78 | True | browse | pt-BR | 2026-03-16 15:59:08.206270 | null | android | home | 1970 | null |  | 76 | null | null | 2438 | SES-773337819c78-000081 | null | email | email/newsletter | 13533 | 2026-03-16 | null |
| search_submit | 1.13.1 | cmp_1352 | email | search_search_submit | 551 | button | CL | CUST049 | active | mobile | EVT-773337819c78-000602 | 2 | click | exp_search_ranking | variant_b | FLW-773337819c78-000081 | 773337819c78 | True | browse | pt-BR | 2026-03-16 15:59:08.206330 | null | android | search | 2800 | null |  | 47 | {"category":"fashion","brand":"Orbit"} | coffee maker | 8770 | SES-773337819c78-000081 | price_desc | email | email/newsletter | 18389 | 2026-03-16 | null |
| landing | 1.14.0 | null | direct | home_landing | 47 | page | AR | CUST033 | active | mobile | EVT-773337819c78-000935 | 1 | click | exp_home_reco | control | FLW-773337819c78-000127 | 773337819c78 | True | browse | pt-BR | 2026-03-16 16:04:05.746847 | null | android | home | 2386 | null | instagram.com | 64 | null | null | 3796 | SES-773337819c78-000127 | null | direct | direct/(none) | 12804 | 2026-03-16 | null |

## `main.silver.customers`

### Schema

| coluna | tipo | nullable |
|---|---|---|
| country | string | True |
| created_at | timestamp | True |
| customer_id | string | True |
| email | string | True |
| first_name | string | True |
| last_name | string | True |
| segment | string | True |

### Sample

| country | created_at | customer_id | email | first_name | last_name | segment |
| --- | --- | --- | --- | --- | --- | --- |
| US | 2025-01-01 12:00:00 | CUST001 | customer001@example.com | Customer001 | Silva001 | active |
| MX | 2025-02-02 12:00:00 | CUST002 | customer002@example.com | Customer002 | Silva002 | vip |
| AR | 2025-03-03 12:00:00 | CUST003 | customer003@example.com | Customer003 | Silva003 | churn_risk |
| CL | 2025-04-04 12:00:00 | CUST004 | customer004@example.com | Customer004 | Silva004 | new |
| BR | 2025-05-05 12:00:00 | CUST005 | customer005@example.com | Customer005 | Silva005 | active |

## `main.silver.products`

### Schema

| coluna | tipo | nullable |
|---|---|---|
| active | boolean | True |
| brand | string | True |
| category | string | True |
| created_at | timestamp | True |
| name | string | True |
| price | double | True |
| product_id | string | True |
| sku | string | True |

### Sample

| active | brand | category | created_at | name | price | product_id | sku |
| --- | --- | --- | --- | --- | --- | --- | --- |
| True | Nexa | electronics | 2025-01-01 09:30:00 | Electronics Item 001 | 18.75 | PROD001 | SKU-ELE-0001 |
| True | Orbit | fashion | 2025-02-02 09:30:00 | Fashion Item 002 | 22.5 | PROD002 | SKU-FAS-0002 |
| True | Pulse | home | 2025-03-03 09:30:00 | Home Item 003 | 26.25 | PROD003 | SKU-HOM-0003 |
| True | Vertex | sports | 2025-04-04 09:30:00 | Sports Item 004 | 30.0 | PROD004 | SKU-SPO-0004 |
| True | Atlas | beauty | 2025-05-05 09:30:00 | Beauty Item 005 | 33.75 | PROD005 | SKU-BEA-0005 |

## `main.silver.purchases`

### Schema

| coluna | tipo | nullable |
|---|---|---|
| amount | double | True |
| campaign_id | string | True |
| channel | string | True |
| coupon_code | string | True |
| cross_sell_source | string | True |
| currency | string | True |
| customer_country | string | True |
| customer_id | string | True |
| customer_segment | string | True |
| device_type | string | True |
| discount_amount | double | True |
| discount_pct | double | True |
| event_id | string | True |
| event_type | string | True |
| flow_id | string | True |
| generator_instance_id | string | True |
| is_first_purchase | boolean | True |
| occurred_at | timestamp | True |
| order_id | string | True |
| order_item_count | int | True |
| order_item_index | int | True |
| order_total_amount | double | True |
| product_brand | string | True |
| product_category | string | True |
| product_id | string | True |
| product_sku | string | True |
| quantity | int | True |
| session_id | string | True |
| source_medium | string | True |
| unit_price | double | True |
| ingest_date | date | True |
| _rescued_data | string | True |

### Sample

| amount | campaign_id | channel | coupon_code | cross_sell_source | currency | customer_country | customer_id | customer_segment | device_type | discount_amount | discount_pct | event_id | event_type | flow_id | generator_instance_id | is_first_purchase | occurred_at | order_id | order_item_count | order_item_index | order_total_amount | product_brand | product_category | product_id | product_sku | quantity | session_id | source_medium | unit_price | ingest_date | _rescued_data |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ | _sem dados_ |

## `main.silver.transactions`

### Schema

| coluna | tipo | nullable |
|---|---|---|
| amount | double | True |
| customer_id | string | True |
| event_id | string | True |
| event_type | string | True |
| flow_id | string | True |
| generator_instance_id | string | True |
| occurred_at | timestamp | True |
| order_id | string | True |
| payment_method | string | True |
| session_id | string | True |
| status | string | True |
| transaction_id | string | True |
| ingest_date | date | True |
| _rescued_data | string | True |

### Sample

| amount | customer_id | event_id | event_type | flow_id | generator_instance_id | occurred_at | order_id | payment_method | session_id | status | transaction_id | ingest_date | _rescued_data |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1410.0 | CUST049 | EVT-773337819c78-000922 | transaction | FLW-773337819c78-000124 | 773337819c78 | 2026-03-16 16:03:52.421954 | ORD-773337819c78-000048 | pix | SES-773337819c78-000124 | authorized | TXN-773337819c78-000048 | 2026-03-16 | null |
| 1537.5 | CUST029 | EVT-773337819c78-001128 | transaction | FLW-773337819c78-000155 | 773337819c78 | 2026-03-16 16:05:54.226742 | ORD-773337819c78-000057 | paypal | SES-773337819c78-000155 | settled | TXN-773337819c78-000057 | 2026-03-16 | null |
| 1076.25 | CUST046 | EVT-773337819c78-000125 | transaction | FLW-773337819c78-000020 | 773337819c78 | 2026-03-16 15:54:35.617601 | ORD-773337819c78-000004 | credit_card | SES-773337819c78-000020 | authorized | TXN-773337819c78-000004 | 2026-03-16 | null |
| 1556.25 | CUST025 | EVT-773337819c78-000308 | transaction | FLW-773337819c78-000045 | 773337819c78 | 2026-03-16 15:56:17.830535 | ORD-773337819c78-000013 | credit_card | SES-773337819c78-000045 | authorized | TXN-773337819c78-000013 | 2026-03-16 | null |
| 266.25 | CUST035 | EVT-773337819c78-000339 | transaction | FLW-773337819c78-000051 | 773337819c78 | 2026-03-16 15:56:42.248618 | ORD-773337819c78-000015 | credit_card | SES-773337819c78-000051 | authorized | TXN-773337819c78-000015 | 2026-03-16 | null |
