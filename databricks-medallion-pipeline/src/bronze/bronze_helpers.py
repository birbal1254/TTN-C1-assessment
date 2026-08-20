"""
Shared utilities for Bronze layer ingestion scripts.

Provides Spark session resolution, path handling, and Delta write helpers
compatible with Databricks Community Edition.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
ENV_DATA_PATH = "BRONZE_DATA_PATH"


@dataclass
class IngestResult:
    """Outcome of a single Bronze ingestion job."""

    table_name: str
    source_path: str
    row_count: int
    elapsed_seconds: float
    success: bool
    error_message: Optional[str] = None


def get_spark(app_name: str) -> SparkSession:
    """
    Return the active Spark session or create one for local execution.

    On Databricks notebooks/jobs, the active session is reused.

    Args:
        app_name: Spark application name when creating a new session.

    Returns:
        Active or newly created SparkSession.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    # Local fallback — Delta extensions required for format("delta")
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def resolve_source_path(filename: str, data_dir: Optional[str] = None) -> str:
    """
    Resolve the full path to a source CSV file.

    Priority: explicit data_dir → BRONZE_DATA_PATH env → project data/ folder.

    Args:
        filename: CSV file name (e.g. customers.csv).
        data_dir: Optional base directory override.

    Returns:
        Absolute or cluster path string for spark.read.csv.
    """
    if data_dir:
        base = Path(data_dir)
    elif os.environ.get(ENV_DATA_PATH):
        base = Path(os.environ[ENV_DATA_PATH])
    else:
        base = DEFAULT_DATA_DIR

    return str(base / filename)


def read_csv(spark: SparkSession, source_path: str) -> DataFrame:
    """
    Read a CSV file with header and inferred schema.

    Args:
        spark: Active SparkSession.
        source_path: Path to the CSV file.

    Returns:
        DataFrame with inferred schema.

    Raises:
        FileNotFoundError: If the path is local and does not exist.
        Exception: Spark read failures (permissions, corrupt file, etc.).
    """
    # Local paths can be checked before Spark read; dbfs:/ and s3:// skip this check
    if not source_path.startswith(("dbfs:", "s3:", "s3a:", "hdfs:")):
        if not Path(source_path).is_file():
            raise FileNotFoundError(f"Source CSV not found: {source_path}")

    return spark.read.csv(
        source_path,
        header=True,
        inferSchema=True,
    )


def add_ingestion_metadata(df: DataFrame, source_path: str) -> DataFrame:
    """
    Append Bronze audit columns without altering business columns.

    Args:
        df: Raw DataFrame read from CSV.
        source_path: Literal source path stored on every row.

    Returns:
        DataFrame with ingestion_timestamp and source_file columns.
    """
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(source_path))
    )


def write_delta_table(df: DataFrame, table_name: str) -> None:
    """
    Write a DataFrame to a Delta table using overwrite for idempotent re-runs.

    overwriteSchema allows CSV schema drift between daily loads without failing
    the job — raw Bronze still reflects the file that arrived.

    Args:
        df: DataFrame to write.
        table_name: Target Delta table name (e.g. bronze_customers).

    Raises:
        Exception: Delta write or schema-related failures.
    """
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        table_name
    )


def ingest_csv_to_bronze(
    spark: SparkSession,
    source_filename: str,
    table_name: str,
    data_dir: Optional[str] = None,
) -> IngestResult:
    """
    Read a CSV, add ingestion metadata, and write to a Bronze Delta table.

    Args:
        spark: Active SparkSession.
        source_filename: CSV file name under the data directory.
        table_name: Target Delta table name.
        data_dir: Optional override for CSV base directory.

    Returns:
        IngestResult with row count and timing on success or failure details.
    """
    source_path = resolve_source_path(source_filename, data_dir)
    start = time.perf_counter()

    try:
        logger.info("Starting Bronze ingest: %s → %s", source_path, table_name)

        raw_df = read_csv(spark, source_path)
        rows_before = raw_df.count()
        logger.info("Row count after read (before metadata): %d", rows_before)
        print(f"[bronze] {table_name}: read {rows_before:,} rows from {source_path}")

        enriched_df = add_ingestion_metadata(raw_df, source_path)
        write_delta_table(enriched_df, table_name)

        rows_after = spark.table(table_name).count()
        elapsed = time.perf_counter() - start
        logger.info(
            "Bronze ingest complete: %s — %d rows written in %.2fs",
            table_name,
            rows_after,
            elapsed,
        )
        print(f"[bronze] {table_name}: wrote {rows_after:,} rows to Delta ({elapsed:.2f}s)")

        if rows_before != rows_after:
            logger.warning(
                "Row count mismatch for %s: read=%d table=%d",
                table_name,
                rows_before,
                rows_after,
            )

        return IngestResult(
            table_name=table_name,
            source_path=source_path,
            row_count=rows_after,
            elapsed_seconds=elapsed,
            success=True,
        )

    except FileNotFoundError as exc:
        elapsed = time.perf_counter() - start
        logger.error("File not found for %s: %s", table_name, exc)
        print(f"[bronze] ERROR {table_name}: file not found — {exc}")
        return IngestResult(
            table_name=table_name,
            source_path=source_path,
            row_count=0,
            elapsed_seconds=elapsed,
            success=False,
            error_message=str(exc),
        )

    except Exception as exc:
        elapsed = time.perf_counter() - start
        error_text = str(exc)
        if "SCHEMA" in error_text.upper() or "schema" in error_text.lower():
            logger.error(
                "Schema mismatch or evolution error for %s: %s",
                table_name,
                exc,
            )
            print(f"[bronze] ERROR {table_name}: schema mismatch — {error_text}")
        else:
            logger.error("Bronze ingest failed for %s: %s", table_name, exc)
            print(f"[bronze] ERROR {table_name}: {exc}")
        return IngestResult(
            table_name=table_name,
            source_path=source_path,
            row_count=0,
            elapsed_seconds=elapsed,
            success=False,
            error_message=error_text,
        )
