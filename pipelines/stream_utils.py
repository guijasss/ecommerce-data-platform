from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

spark = SparkSession.builder.getOrCreate()

DEFAULT_SCHEMA_EVOLUTION_MODE = "addNewColumns"


def _try_get_dbutils():
    # Databricks notebooks/runtime usually has DBUtils available; keep this optional for local linting/imports.
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        return DBUtils(spark)
    except Exception:
        return None


def reset_stream_target(
    *,
    table_name: str | None = None,
    output_path: str | None = None,
    checkpoint_location: str | None = None,
    extra_paths: list[str] | None = None,
) -> None:
    """
    Reset only the *target* side of a streaming pipeline.

    Typical usage before a full reprocess:
    - Drop target table (if any)
    - Remove checkpoint (required so the stream reads everything again)
    - Remove target data path (so the writer recreates from scratch)

    Notes:
    - This intentionally does NOT touch the source (raw/bronze) paths or Auto Loader schemaLocation.
    - Requires Databricks runtime for fs deletion; outside Databricks it becomes a no-op for paths.
    """
    if table_name:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    dbutils = _try_get_dbutils()
    if not dbutils:
        return

    paths: list[str] = []
    if checkpoint_location:
        paths.append(checkpoint_location)
    if output_path:
        paths.append(output_path)
    if extra_paths:
        paths.extend([p for p in extra_paths if p])

    for p in paths:
        try:
            dbutils.fs.rm(p, True)
        except Exception:
            # Best-effort: path may not exist or user may not have permissions.
            pass


def get_stream_dataframe(df: DataFrame, dbutils: Any) -> DataFrame:
    tmp_checkpoint_location = "/Volumes/main/tmp/checkpoints/autoloader_preview"
    dbutils.fs.rm(tmp_checkpoint_location, recurse=True)

    (
        df.writeStream
        .format("memory")
        .queryName("autoloader_preview")
        .option("checkpointLocation", tmp_checkpoint_location)
        .trigger(availableNow=True)
        .start()
    )

    return spark.table("autoloader_preview")


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
    output_path: str | None = None,
):
    writer = (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", str(merge_schema).lower())
        .trigger(availableNow=True)
    )

    if output_path:
        # If the target table doesn't exist yet, this allows creation at a specific UC Volume path.
        writer = writer.option("path", output_path)

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
    output_path: str | None = None,
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
        output_path=output_path,
    )
    return writer.toTable(table_name)
