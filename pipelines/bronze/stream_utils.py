from typing import TypedDict, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from pyspark.sql.connect.streaming.readwriter import DataStreamReader, DataStreamWriter

spark = SparkSession.builder.getOrCreate()

DEFAULT_SCHEMA_EVOLUTION_MODE = "addNewColumns"

AutoLoaderReadOptions = TypedDict(
    "AutoLoaderReadOptions",
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

DeltaReadOptions = TypedDict(
    "DeltaReadOptions",
    {
        "ignoreDeletes": bool,
        "ignoreChanges": bool,
        "skipChangeCommits": bool,
        "maxFilesPerTrigger": int,
        "maxBytesPerTrigger": str,
        "startingVersion": str,
        "startingTimestamp": str,
    },
    total=False,
)


class DeltaWriteOptions(TypedDict, total=False):
    outputMode: str
    partitionBy: list[str]
    queryName: str
    mergeSchema: bool
    extraOptions: dict[str, str]


def _apply_options(reader_or_writer, options: dict) -> Union[DataStreamReader, DataStreamWriter]:
    for key, value in options.items():
        if value is None:
            continue

        if isinstance(value, bool):
            value = str(value).lower()

        reader_or_writer = reader_or_writer.option(key, value)

    return reader_or_writer


def read_autoloader_stream(
    source_path: str,
    schema_location: str,
    schema: StructType | None = None,
    schema_evolution_mode: str = DEFAULT_SCHEMA_EVOLUTION_MODE,
    options: AutoLoaderReadOptions | None = None,
) -> DataFrame:
    options = options or {}

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.schemaLocation", schema_location)
    )

    autoloader_options = {
        "cloudFiles.format": "json",
        "rescuedDataColumn": "_rescued_data",
        "cloudFiles.includeExistingFiles": True,
        "cloudFiles.allowOverwrites": False,
        "cloudFiles.useNotifications": False,
        **options,
    }
    autoloader_options.pop("addIngestionMetadata", None)

    if schema is not None:
        autoloader_options.pop("cloudFiles.schemaHints", None)

    reader = _apply_options(reader, autoloader_options)

    if schema is None:
        reader = reader.option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
    else:
        reader = reader.schema(schema)

    return reader.load(source_path)


def read_delta_path_stream(
    source_path: str,
    options: DeltaReadOptions | None = None,
) -> DataFrame:
    reader = spark.readStream.format("delta")
    reader = _apply_options(reader, options or {})
    return reader.load(source_path)


def read_delta_table_stream(
    table_name: str,
    options: DeltaReadOptions | None = None,
) -> DataFrame:
    reader = spark.readStream.format("delta")
    reader = _apply_options(reader, options or {})
    return reader.table(table_name)


def _build_delta_writer(
    df: DataFrame,
    checkpoint_location: str,
    options: DeltaWriteOptions | None = None,
):
    options = options or {}

    writer = (
        df.writeStream.format("delta")
        .outputMode(options.get("outputMode", "append"))
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", str(options.get("mergeSchema", True)).lower())
        .trigger(availableNow=True)
    )

    if "queryName" in options:
        writer = writer.queryName(options["queryName"])

    if "partitionBy" in options:
        writer = writer.partitionBy(*options["partitionBy"])

    writer = _apply_options(writer, options.get("extraOptions", {}))
    return writer


def write_delta_path_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_location: str,
    options: DeltaWriteOptions | None = None,
) -> StreamingQuery:
    writer = _build_delta_writer(df, checkpoint_location, options)
    return writer.start(output_path)


def write_delta_table_stream(
    df: DataFrame,
    table_name: str,
    checkpoint_location: str,
    options: DeltaWriteOptions | None = None,
) -> StreamingQuery:
    writer = _build_delta_writer(df, checkpoint_location, options)
    return writer.toTable(table_name)
