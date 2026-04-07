from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, get_json_object, lit, to_json, to_timestamp

from pipelines.silver.common import add_standard_metadata, cleaned_string, deduplicate_stream, setup_reprocess_widget
from pipelines.stream_utils import read_autoloader_stream, reset_stream_target, spark, write_delta_table_stream


DECIMAL_18_2 = "decimal(18,2)"
STREAM_ENTITY = "fact_event_raw"
TABLE_NAME = "main.silver.fact_event_raw"
TARGET_PATH = f"/Volumes/main/silver/ecommerce/{STREAM_ENTITY}"
CHECKPOINT_LOCATION = f"/Volumes/main/silver/ecommerce/_checkpoints/{STREAM_ENTITY}"
QUERY_NAME = f"silver_{STREAM_ENTITY}_available_now"


def transform_fact_event_raw(df: DataFrame) -> DataFrame:
    attributes_json = to_json(col("attributes"))
    silver_df = (
        df.withColumn("event_ts", to_timestamp(col("event_ts")))
        .withColumn("event_id", cleaned_string("event_id"))
        .withColumn("event_type", cleaned_string("event_type", lower_case=True))
        .withColumn("session_id", cleaned_string("session_id"))
        .withColumn("customer_id", cleaned_string("customer_id"))
        .withColumn("product_id", cleaned_string("product_id"))
        .withColumn("cart_id", cleaned_string("cart_id"))
        .withColumn("order_id", cleaned_string("order_id"))
        .withColumn("campaign_id", cleaned_string("campaign_id"))
        .withColumn("device_type", cleaned_string("device_type", lower_case=True))
        .withColumn("platform", cleaned_string("platform", lower_case=True))
        .withColumn("page_type", cleaned_string("page_type", lower_case=True))
        .withColumn("referrer", cleaned_string("referrer", lower_case=True))
        .withColumn("currency", cleaned_string("currency", upper_case=True))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast(DECIMAL_18_2))
        .withColumn("attributes_json", attributes_json)
        .withColumn("flow_id", get_json_object(attributes_json, "$.flow_id"))
    )
    silver_df = (
        silver_df.withColumn("channel", get_json_object(col("attributes_json"), "$.channel"))
        .withColumn("source", get_json_object(col("attributes_json"), "$.source"))
        .withColumn("medium", get_json_object(col("attributes_json"), "$.medium"))
        .withColumn("customer_segment_seed", get_json_object(col("attributes_json"), "$.customer_segment_seed"))
        .withColumn("step_index", get_json_object(col("attributes_json"), "$.step_index").cast("int"))
        .withColumn("search_query", coalesce(get_json_object(col("attributes_json"), "$.query"), get_json_object(col("attributes_json"), "$.search_query")))
        .withColumn("results_category", get_json_object(col("attributes_json"), "$.results_category"))
        .withColumn("category_name", get_json_object(col("attributes_json"), "$.category_name"))
        .withColumn("brand", get_json_object(col("attributes_json"), "$.brand"))
        .withColumn("cart_item_count", get_json_object(col("attributes_json"), "$.cart_item_count").cast("int"))
        .withColumn("cart_value", get_json_object(col("attributes_json"), "$.cart_value").cast(DECIMAL_18_2))
        .withColumn("distinct_items", get_json_object(col("attributes_json"), "$.distinct_items").cast("int"))
        .withColumn("payment_method", get_json_object(col("attributes_json"), "$.payment_method"))
        .withColumn("attempted_amount", get_json_object(col("attributes_json"), "$.attempted_amount").cast(DECIMAL_18_2))
        .withColumn("gross_amount", get_json_object(col("attributes_json"), "$.gross_amount").cast(DECIMAL_18_2))
        .withColumn("discount_amount", get_json_object(col("attributes_json"), "$.discount_amount").cast(DECIMAL_18_2))
        .withColumn("shipping_amount", get_json_object(col("attributes_json"), "$.shipping_amount").cast(DECIMAL_18_2))
        .withColumn("tax_amount", get_json_object(col("attributes_json"), "$.tax_amount").cast(DECIMAL_18_2))
        .withColumn("net_amount", get_json_object(col("attributes_json"), "$.net_amount").cast(DECIMAL_18_2))
        .withColumn("item_count", get_json_object(col("attributes_json"), "$.item_count").cast("int"))
        .withColumn("distinct_skus", get_json_object(col("attributes_json"), "$.distinct_skus").cast("int"))
        .withColumn("reason", get_json_object(col("attributes_json"), "$.reason"))
        .withColumn("refund_amount", get_json_object(col("attributes_json"), "$.refund_amount").cast(DECIMAL_18_2))
        .withColumn("return_resolution", get_json_object(col("attributes_json"), "$.return_resolution"))
        .withColumn("ticket_reason", get_json_object(col("attributes_json"), "$.ticket_reason"))
        .withColumn("priority", get_json_object(col("attributes_json"), "$.priority"))
        .withColumn("delivery_lead_time_hours", get_json_object(col("attributes_json"), "$.delivery_lead_time_hours").cast("double"))
        .withColumn("promised_sla_days", get_json_object(col("attributes_json"), "$.promised_sla_days").cast("int"))
        .withColumn("order_items_json", get_json_object(col("attributes_json"), "$.order_items"))
        .withColumn(
            "event_amount",
            coalesce(
                col("net_amount"),
                col("gross_amount"),
                col("attempted_amount"),
                col("refund_amount"),
                (coalesce(col("unit_price"), lit(0).cast(DECIMAL_18_2)) * coalesce(col("quantity"), lit(0))).cast(DECIMAL_18_2),
            ),
        )
        .withColumn("is_commerce_event", col("event_type").isin("add_to_cart", "remove_from_cart", "checkout_started", "order_placed"))
        .withColumn("is_fulfillment_event", col("event_type").isin("shipment_created", "shipment_delivered"))
        .withColumn("is_return_event", col("event_type").isin("return_requested", "return_completed"))
        .withColumn("is_support_event", col("event_type") == lit("support_ticket_created"))
        .withColumn("graph_customer_id", col("customer_id"))
        .withColumn("graph_product_id", col("product_id"))
        .withColumn("graph_order_id", col("order_id"))
        .withColumn("graph_interaction_type", col("event_type"))
        .drop("attributes")
    )
    silver_df = (
        silver_df.withColumn("flow_id", cleaned_string("flow_id"))
        .withColumn("channel", cleaned_string("channel", lower_case=True))
        .withColumn("source", cleaned_string("source", lower_case=True))
        .withColumn("medium", cleaned_string("medium", lower_case=True))
        .withColumn("customer_segment_seed", cleaned_string("customer_segment_seed", lower_case=True))
        .withColumn("search_query", cleaned_string("search_query"))
        .withColumn("results_category", cleaned_string("results_category", lower_case=True))
        .withColumn("category_name", cleaned_string("category_name", lower_case=True))
        .withColumn("brand", cleaned_string("brand"))
        .withColumn("payment_method", cleaned_string("payment_method", lower_case=True))
        .withColumn("reason", cleaned_string("reason", lower_case=True))
        .withColumn("return_resolution", cleaned_string("return_resolution", lower_case=True))
        .withColumn("ticket_reason", cleaned_string("ticket_reason", lower_case=True))
        .withColumn("priority", cleaned_string("priority", lower_case=True))
    )
    silver_df = add_standard_metadata(silver_df, timestamp_column="event_ts", date_column="event_date")
    return deduplicate_stream(silver_df, timestamp_column="event_ts")


spark.sql("CREATE SCHEMA IF NOT EXISTS main.silver")

if setup_reprocess_widget():
    reset_stream_target(
        table_name=TABLE_NAME,
        output_path=TARGET_PATH,
        checkpoint_location=CHECKPOINT_LOCATION,
    )

source_stream_df = read_autoloader_stream(
    stream_entity=STREAM_ENTITY,
    schema_evolution_mode="addNewColumns",
)
silver_stream_df = transform_fact_event_raw(source_stream_df)

query = write_delta_table_stream(
    df=silver_stream_df,
    table_name=TABLE_NAME,
    output_path=TARGET_PATH,
    checkpoint_location=CHECKPOINT_LOCATION,
    query_name=QUERY_NAME,
    partition_by=["event_date"],
    merge_schema=True,
)
query.awaitTermination()
