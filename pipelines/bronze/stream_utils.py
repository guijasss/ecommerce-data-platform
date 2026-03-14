from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

spark = SparkSession.builder.getOrCreate()


def read_bronze_stream(
    source_path: str,
    schema_location: str,
    file_format: str = "json",
    extra_options: dict | None = None,
) -> DataFrame:
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
    )

    if extra_options:
        for key, value in extra_options.items():
            reader = reader.option(key, value)

    return reader.load(source_path)


def write_silver_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_location: str,
    output_mode: str = "append",
    partition_by: list[str] | None = None,
) -> StreamingQuery:
    writer = (
        df.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    return writer.start(output_path)
