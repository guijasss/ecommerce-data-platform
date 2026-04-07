from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, to_timestamp

from pipelines.silver.common import add_standard_metadata, cleaned_string, deduplicate_stream, setup_reprocess_widget
from pipelines.stream_utils import read_autoloader_stream, reset_stream_target, spark, write_delta_table_stream


DECIMAL_18_2 = "decimal(18,2)"
DECIMAL_5_2 = "decimal(5,2)"
STREAM_ENTITY = "purchases"
TABLE_NAME = "main.silver.purchases"
TARGET_PATH = f"/Volumes/main/silver/ecommerce/{STREAM_ENTITY}"
CHECKPOINT_LOCATION = f"/Volumes/main/silver/ecommerce/_checkpoints/{STREAM_ENTITY}"
QUERY_NAME = f"silver_{STREAM_ENTITY}_available_now"


def transform_purchases(df: DataFrame) -> DataFrame:
    silver_df = (
        df.withColumn("occurred_at", to_timestamp(col("occurred_at")))
        .withColumn("event_id", cleaned_string("event_id"))
        .withColumn("event_type", cleaned_string("event_type", lower_case=True))
        .withColumn("generator_instance_id", cleaned_string("generator_instance_id"))
        .withColumn("customer_id", cleaned_string("customer_id"))
        .withColumn("customer_country", cleaned_string("customer_country", upper_case=True))
        .withColumn("customer_segment", cleaned_string("customer_segment", lower_case=True))
        .withColumn("session_id", cleaned_string("session_id"))
        .withColumn("order_id", cleaned_string("order_id"))
        .withColumn("product_id", cleaned_string("product_id"))
        .withColumn("product_sku", cleaned_string("product_sku"))
        .withColumn("product_category", cleaned_string("product_category", lower_case=True))
        .withColumn("product_brand", cleaned_string("product_brand"))
        .withColumn("flow_id", cleaned_string("flow_id"))
        .withColumn("channel", cleaned_string("channel", lower_case=True))
        .withColumn("campaign_id", cleaned_string("campaign_id"))
        .withColumn("source_medium", cleaned_string("source_medium", lower_case=True))
        .withColumn("device_type", cleaned_string("device_type", lower_case=True))
        .withColumn("coupon_code", cleaned_string("coupon_code", upper_case=True))
        .withColumn("currency", cleaned_string("currency", upper_case=True))
        .withColumn("cross_sell_source", cleaned_string("cross_sell_source", lower_case=True))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast(DECIMAL_18_2))
        .withColumn("amount", col("amount").cast(DECIMAL_18_2))
        .withColumn("discount_amount", col("discount_amount").cast(DECIMAL_18_2))
        .withColumn("discount_pct", col("discount_pct").cast(DECIMAL_5_2))
        .withColumn("order_total_amount", col("order_total_amount").cast(DECIMAL_18_2))
        .withColumn("order_item_count", col("order_item_count").cast("int"))
        .withColumn("order_item_index", col("order_item_index").cast("int"))
        .withColumn("is_first_purchase", col("is_first_purchase").cast("boolean"))
        .withColumn("net_item_amount", (coalesce(col("amount"), lit(0).cast(DECIMAL_18_2)) - coalesce(col("discount_amount"), lit(0).cast(DECIMAL_18_2))).cast(DECIMAL_18_2))
        .withColumn("gross_item_amount", (coalesce(col("unit_price"), lit(0).cast(DECIMAL_18_2)) * coalesce(col("quantity"), lit(0))).cast(DECIMAL_18_2))
        .withColumn("is_discounted", coalesce(col("discount_amount"), lit(0).cast(DECIMAL_18_2)) > lit(0).cast(DECIMAL_18_2))
        .withColumn("is_repeat_purchase", ~coalesce(col("is_first_purchase"), lit(False)))
        .withColumn("graph_customer_id", col("customer_id"))
        .withColumn("graph_product_id", col("product_id"))
        .withColumn("graph_order_id", col("order_id"))
    )
    silver_df = add_standard_metadata(silver_df, timestamp_column="occurred_at", date_column="occurred_date")
    return deduplicate_stream(silver_df, timestamp_column="occurred_at")


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
silver_stream_df = transform_purchases(source_stream_df)

query = write_delta_table_stream(
    df=silver_stream_df,
    table_name=TABLE_NAME,
    output_path=TARGET_PATH,
    checkpoint_location=CHECKPOINT_LOCATION,
    query_name=QUERY_NAME,
    partition_by=["occurred_date"],
    merge_schema=True,
)
query.awaitTermination()
