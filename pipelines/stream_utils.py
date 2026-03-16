from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

spark = SparkSession.builder.getOrCreate()

DEFAULT_SCHEMA_EVOLUTION_MODE = "addNewColumns"


def read_autoloader_stream(
    stream_entity: str,
    file_format: str = "json",
    schema: StructType | None = None,
    schema_evolution_mode: str = DEFAULT_SCHEMA_EVOLUTION_MODE,
    include_existing_files: bool = True,
    rescued_data_column: str = "_rescued_data",
) -> DataFrame:
    schema_location = f"/Volumes/main/bronze/ecommerce/_schemas/{stream_entity}"
    source_path = f"/Volumes/main/bronze/ecommerce/{stream_entity}"

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.includeExistingFiles", str(include_existing_files).lower())
        .option("rescuedDataColumn", rescued_data_column)
    )

    if schema is None:
        reader = reader.option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
    else:
        reader = reader.schema(schema)

    return reader.load(source_path)


def read_delta_path_stream(source_path: str) -> DataFrame:
    return spark.readStream.format("delta").load(source_path)


def read_delta_table_stream(table_name: str) -> DataFrame:
    return spark.readStream.format("delta").table(table_name)


def _build_delta_writer(
    df: DataFrame,
    checkpoint_location: str,
    output_mode: str = "append",
    partition_by: list[str] | None = None,
    query_name: str | None = None,
    merge_schema: bool = True,
):
    writer = (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", str(merge_schema).lower())
        .trigger(availableNow=True)
    )

    if query_name:
        writer = writer.queryName(query_name)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    return writer


def write_delta_path_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_location: str,
    output_mode: str = "append",
    partition_by: list[str] | None = None,
    query_name: str | None = None,
    merge_schema: bool = True,
) -> StreamingQuery:
    writer = _build_delta_writer(
        df=df,
        checkpoint_location=checkpoint_location,
        output_mode=output_mode,
        partition_by=partition_by,
        query_name=query_name,
        merge_schema=merge_schema,
    )
    return writer.start(output_path)


def write_delta_table_stream(
    df: DataFrame,
    table_name: str,
    checkpoint_location: str,
    output_mode: str = "append",
    partition_by: list[str] | None = None,
    query_name: str | None = None,
    merge_schema: bool = True,
) -> StreamingQuery:
    writer = _build_delta_writer(
        df=df,
        checkpoint_location=checkpoint_location,
        output_mode=output_mode,
        partition_by=partition_by,
        query_name=query_name,
        merge_schema=merge_schema,
    )
    return writer.toTable(table_name)
