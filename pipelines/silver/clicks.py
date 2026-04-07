from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_json, to_timestamp

from pipelines.silver.common import add_standard_metadata, cleaned_string, deduplicate_stream, setup_reprocess_widget
from pipelines.stream_utils import read_autoloader_stream, reset_stream_target, spark, write_delta_table_stream


STREAM_ENTITY = "clicks"
TABLE_NAME = "main.silver.clicks"
TARGET_PATH = f"/Volumes/main/silver/ecommerce/{STREAM_ENTITY}"
CHECKPOINT_LOCATION = f"/Volumes/main/silver/ecommerce/_checkpoints/{STREAM_ENTITY}"
QUERY_NAME = f"silver_{STREAM_ENTITY}_available_now"


def transform_clicks(df: DataFrame) -> DataFrame:
    silver_df = (
        df.withColumn("occurred_at", to_timestamp(col("occurred_at")))
        .withColumn("event_id", cleaned_string("event_id"))
        .withColumn("event_type", cleaned_string("event_type", lower_case=True))
        .withColumn("generator_instance_id", cleaned_string("generator_instance_id"))
        .withColumn("customer_id", cleaned_string("customer_id"))
        .withColumn("customer_country", cleaned_string("customer_country", upper_case=True))
        .withColumn("customer_segment", cleaned_string("customer_segment", lower_case=True))
        .withColumn("session_id", cleaned_string("session_id"))
        .withColumn("product_id", cleaned_string("product_id"))
        .withColumn("page", cleaned_string("page", lower_case=True))
        .withColumn("action", cleaned_string("action", lower_case=True))
        .withColumn("flow_id", cleaned_string("flow_id"))
        .withColumn("source", cleaned_string("source", lower_case=True))
        .withColumn("channel", cleaned_string("channel", lower_case=True))
        .withColumn("campaign_id", cleaned_string("campaign_id"))
        .withColumn("source_medium", cleaned_string("source_medium", lower_case=True))
        .withColumn("referrer", cleaned_string("referrer", lower_case=True))
        .withColumn("device_type", cleaned_string("device_type", lower_case=True))
        .withColumn("os", cleaned_string("os", lower_case=True))
        .withColumn("app_version", cleaned_string("app_version"))
        .withColumn("locale", cleaned_string("locale", lower_case=True))
        .withColumn("experiment_id", cleaned_string("experiment_id"))
        .withColumn("experiment_variant", cleaned_string("experiment_variant", lower_case=True))
        .withColumn("journey_step", cleaned_string("journey_step", lower_case=True))
        .withColumn("onboarding_step", cleaned_string("onboarding_step", lower_case=True))
        .withColumn("component_id", cleaned_string("component_id"))
        .withColumn("component_type", cleaned_string("component_type", lower_case=True))
        .withColumn("search_query", cleaned_string("search_query"))
        .withColumn("sort_order", cleaned_string("sort_order", lower_case=True))
        .withColumn("recommendation_model", cleaned_string("recommendation_model", lower_case=True))
        .withColumn("recommendation_source", cleaned_string("recommendation_source", lower_case=True))
        .withColumn("recommendation_request_id", cleaned_string("recommendation_request_id"))
        .withColumn("seed_product_id", cleaned_string("seed_product_id"))
        .withColumn("is_logged_in", col("is_logged_in").cast("boolean"))
        .withColumn("event_position", col("event_position").cast("int"))
        .withColumn("recommendation_rank", col("recommendation_rank").cast("int"))
        .withColumn("page_load_time_ms", col("page_load_time_ms").cast("int"))
        .withColumn("component_load_time_ms", col("component_load_time_ms").cast("int"))
        .withColumn("time_on_previous_page_ms", col("time_on_previous_page_ms").cast("int"))
        .withColumn("session_elapsed_ms", col("session_elapsed_ms").cast("int"))
        .withColumn("scroll_depth_pct", col("scroll_depth_pct").cast("int"))
        .withColumn("product_impression_count", col("product_impression_count").cast("int"))
        .withColumn("search_filters_json", to_json(col("search_filters")))
        .withColumn("has_product_context", col("product_id").isNotNull())
        .withColumn("is_search_interaction", (col("page") == lit("search")) | col("search_query").isNotNull())
        .withColumn("is_recommendation_interaction", col("recommendation_request_id").isNotNull())
        .withColumn("is_experiment_interaction", col("experiment_id").isNotNull())
        .withColumn("graph_target_product_id", col("product_id"))
        .withColumn("graph_interaction_type", col("action"))
        .drop("search_filters")
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
silver_stream_df = transform_clicks(source_stream_df)

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
