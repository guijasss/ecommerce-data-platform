from typing import TypedDict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

spark = SparkSession.builder.getOrCreate()

DEFAULT_SCHEMA_EVOLUTION_MODE = "addNewColumns"


StreamReadOptions = TypedDict(
    "StreamReadOptions",
    {
        "cloudFiles.format": str,
        "rescuedDataColumn": str,
        "cloudFiles.schemaHints": str,
        "cloudFiles.includeExistingFiles": bool,
        "cloudFiles.allowOverwrites": bool,
        "cloudFiles.useNotifications": bool,
        "maxFilesPerTrigger": int,
        "maxBytesPerTrigger": str,
        "addIngestionMetadata": bool,
    },
    total=False,
)


class StreamWriteOptions(TypedDict, total=False):
    output_mode: str
    partition_by: list[str]
    query_name: str
    mergeSchema: bool
    extra_write_options: dict[str, str]


def read_stream(
    source_path: str,
    schema_location: str,
    schema: StructType | None = None,
    schema_evolution_mode: str = DEFAULT_SCHEMA_EVOLUTION_MODE,
    options: StreamReadOptions | None = None,
) -> DataFrame:
    options = options or {}
    add_ingestion_metadata = options.get("addIngestionMetadata", True)

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.schemaLocation", schema_location)
    )

    normalized_options = {
        "cloudFiles.format": "json",
        "rescuedDataColumn": "_rescued_data",
        "cloudFiles.includeExistingFiles": True,
        "cloudFiles.allowOverwrites": False,
        "cloudFiles.useNotifications": False,
        **options,
    }

    for key, value in normalized_options.items():
        if key == "addIngestionMetadata":
            continue

        if key == "cloudFiles.schemaHints" and schema is not None:
            continue

        if value is None:
            continue

        if isinstance(value, bool):
            value = str(value).lower()

        reader = reader.option(key, value)

    if schema is None:
        reader = reader.option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
    else:
        reader = reader.schema(schema)

    df = reader.load(source_path)

    if add_ingestion_metadata:
        df = (
            df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
        )

    return df


def write_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_location: str,
    options: StreamWriteOptions | None = None,
) -> StreamingQuery:
    options = options or {}

    writer = (
        df.writeStream.format("delta")
        .outputMode(options.get("output_mode", "append"))
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", str(options.get("mergeSchema", True)).lower())
        .trigger(availableNow=True)
    )

    if "query_name" in options:
        writer = writer.queryName(options["query_name"])

    if "partition_by" in options:
        writer = writer.partitionBy(*options["partition_by"])

    for key, value in options.get("extra_write_options", {}).items():
        writer = writer.option(key, value)

    return writer.start(output_path)
