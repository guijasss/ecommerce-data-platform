from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_timestamp, when

from pipelines.silver.common import add_standard_metadata, cleaned_string, deduplicate_stream, setup_reprocess_widget
from pipelines.stream_utils import read_autoloader_stream, reset_stream_target, spark, write_delta_table_stream


DECIMAL_18_2 = "decimal(18,2)"
STREAM_ENTITY = "transactions"
TABLE_NAME = "main.silver.transactions"
TARGET_PATH = f"/Volumes/main/silver/ecommerce/{STREAM_ENTITY}"
CHECKPOINT_LOCATION = f"/Volumes/main/silver/ecommerce/_checkpoints/{STREAM_ENTITY}"
QUERY_NAME = f"silver_{STREAM_ENTITY}_available_now"


def transform_transactions(df: DataFrame) -> DataFrame:
    silver_df = (
        df.withColumn("occurred_at", to_timestamp(col("occurred_at")))
        .withColumn("event_id", cleaned_string("event_id"))
        .withColumn("event_type", cleaned_string("event_type", lower_case=True))
        .withColumn("generator_instance_id", cleaned_string("generator_instance_id"))
        .withColumn("customer_id", cleaned_string("customer_id"))
        .withColumn("session_id", cleaned_string("session_id"))
        .withColumn("transaction_id", cleaned_string("transaction_id"))
        .withColumn("order_id", cleaned_string("order_id"))
        .withColumn("payment_method", cleaned_string("payment_method", lower_case=True))
        .withColumn("status", cleaned_string("status", lower_case=True))
        .withColumn("flow_id", cleaned_string("flow_id"))
        .withColumn("amount", col("amount").cast(DECIMAL_18_2))
        .withColumn("is_successful", col("status").isin("success", "succeeded", "approved", "paid"))
        .withColumn("is_failed", col("status").isin("failed", "declined", "rejected"))
        .withColumn(
            "status_group",
            when(col("status").isin("success", "succeeded", "approved", "paid"), lit("successful"))
            .when(col("status").isin("failed", "declined", "rejected"), lit("failed"))
            .otherwise(lit("other")),
        )
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
silver_stream_df = transform_transactions(source_stream_df)

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
