from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    hour,
    input_file_name,
    lower,
    to_date,
    trim,
    upper,
    when,
    year,
)


def cleaned_string(column_name: str, *, lower_case: bool = False, upper_case: bool = False) -> Column:
    value = trim(col(column_name))
    value = when(value == "", None).otherwise(value)
    if lower_case:
        value = lower(value)
    if upper_case:
        value = upper(value)
    return value


def add_standard_metadata(df: DataFrame, *, timestamp_column: str, date_column: str) -> DataFrame:
    return (
        df.withColumn(date_column, to_date(col(timestamp_column)))
        .withColumn("event_hour", hour(col(timestamp_column)))
        .withColumn("event_year", year(col(timestamp_column)))
        .withColumn("source_file", input_file_name())
        .withColumn("silver_processed_at", current_timestamp())
    )


def deduplicate_stream(df: DataFrame, *, timestamp_column: str) -> DataFrame:
    return df.withWatermark(timestamp_column, "7 days").dropDuplicates(["event_id"])


def setup_reprocess_widget() -> bool:
    try:
        dbutils.widgets.dropdown("reprocess", "false", ["false", "true"])
        return dbutils.widgets.get("reprocess").lower() == "true"
    except Exception:
        return False
