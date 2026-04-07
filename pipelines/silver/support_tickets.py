from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp

from pipelines.silver.common import add_standard_metadata, cleaned_string, deduplicate_stream, setup_reprocess_widget
from pipelines.stream_utils import read_autoloader_stream, reset_stream_target, spark, write_delta_table_stream


STREAM_ENTITY = "support_tickets"
TABLE_NAME = "main.silver.support_tickets"
TARGET_PATH = f"/Volumes/main/silver/ecommerce/{STREAM_ENTITY}"
CHECKPOINT_LOCATION = f"/Volumes/main/silver/ecommerce/_checkpoints/{STREAM_ENTITY}"
QUERY_NAME = f"silver_{STREAM_ENTITY}_available_now"


def transform_support_tickets(df: DataFrame) -> DataFrame:
    silver_df = (
        df.withColumn("occurred_at", to_timestamp(col("occurred_at")))
        .withColumn("event_id", cleaned_string("event_id"))
        .withColumn("event_type", cleaned_string("event_type", lower_case=True))
        .withColumn("ticket_id", cleaned_string("ticket_id"))
        .withColumn("customer_id", cleaned_string("customer_id"))
        .withColumn("category", cleaned_string("category", lower_case=True))
        .withColumn("priority", cleaned_string("priority", lower_case=True))
        .withColumn("status", cleaned_string("status", lower_case=True))
        .withColumn("is_open", col("status").isin("new", "open", "pending"))
        .withColumn("is_resolved", col("status").isin("resolved", "closed"))
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
silver_stream_df = transform_support_tickets(source_stream_df)

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
