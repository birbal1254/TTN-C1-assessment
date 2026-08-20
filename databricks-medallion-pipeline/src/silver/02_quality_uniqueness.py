"""
Silver layer: data quality check — uniqueness.

Reads Bronze Delta tables and flags duplicate primary keys using Window functions.
The first row per key (by ingestion_timestamp) receives PASS; later duplicates
receive FAIL: DUPLICATE {field_name}. All rows are retained — nothing is filtered.

Checks:
  - bronze_customers: customer_id unique
  - bronze_orders: order_id unique

Databricks Community Edition compatible.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, count, lit, row_number, when

logger = logging.getLogger(__name__)

APP_NAME = "silver_quality_uniqueness"

CUSTOMERS_TABLE = "bronze_customers"
ORDERS_TABLE = "bronze_orders"

CUSTOMER_PK = "customer_id"
ORDER_PK = "order_id"
INGESTION_TIMESTAMP = "ingestion_timestamp"


@dataclass
class UniquenessMetric:
    """Uniqueness statistics for one table and primary key field."""

    table_name: str
    field_name: str
    total_rows: int
    duplicate_rows: int

    @property
    def pass_rate(self) -> float:
        """Return pass rate as a percentage (0–100)."""
        if self.total_rows == 0:
            return 100.0
        return (self.total_rows - self.duplicate_rows) / self.total_rows * 100.0


def get_spark() -> SparkSession:
    """
    Return the active Spark session or create one for local execution.

    Returns:
        Active or newly created SparkSession.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    return (
        SparkSession.builder.appName(APP_NAME)
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


def load_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Load a Bronze Delta table.

    Args:
        spark: Active SparkSession.
        table_name: Bronze table name.

    Returns:
        Bronze table DataFrame.

    Raises:
        Exception: If the table cannot be read.
    """
    try:
        df = spark.table(table_name)
        logger.info("Loaded table %s", table_name)
        return df
    except Exception as exc:
        logger.error("Failed to load Bronze table %s: %s", table_name, exc)
        raise


def flag_uniqueness(
    df: DataFrame,
    primary_key: str,
    ingestion_timestamp_col: str = INGESTION_TIMESTAMP,
) -> DataFrame:
    """
    Flag duplicate primary keys using row_number over a Window partition.

    First occurrence (row_number == 1) → PASS.
    Subsequent duplicates (row_number > 1) → FAIL: DUPLICATE {primary_key}.

    Args:
        df: Input DataFrame (all rows retained).
        primary_key: Primary key column name.
        ingestion_timestamp_col: Column used to determine first occurrence.

    Returns:
        DataFrame with quality_uniqueness column added.
    """
    # Tie-break duplicates by ingestion time so the earliest landed row passes
    window_spec = Window.partitionBy(col(primary_key)).orderBy(
        col(ingestion_timestamp_col).asc_nulls_last()
    )

    ranked_df = df.withColumn("_row_number", row_number().over(window_spec))

    flagged_df = ranked_df.withColumn(
        "quality_uniqueness",
        when(
            col("_row_number") == 1,
            lit("PASS"),
        ).otherwise(lit(f"FAIL: DUPLICATE {primary_key}")),
    ).drop("_row_number")

    return flagged_df


def compute_uniqueness_metric(
    flagged_df: DataFrame,
    table_name: str,
    field_name: str,
) -> UniquenessMetric:
    """
    Compute duplicate row counts from a flagged DataFrame.

    Duplicate rows are those with quality_uniqueness != PASS (row_number > 1).

    Args:
        flagged_df: DataFrame including quality_uniqueness column.
        table_name: Logical table name for reporting.
        field_name: Primary key field name.

    Returns:
        UniquenessMetric with total and duplicate row counts.
    """
    summary = flagged_df.agg(
        count(lit(1)).alias("total_rows"),
        count(when(col("quality_uniqueness") != lit("PASS"), lit(1))).alias(
            "duplicate_rows"
        ),
    ).collect()[0]

    return UniquenessMetric(
        table_name=table_name,
        field_name=field_name,
        total_rows=int(summary["total_rows"]),
        duplicate_rows=int(summary["duplicate_rows"]),
    )


def print_uniqueness_metric(metric: UniquenessMetric) -> None:
    """
    Print one uniqueness metric line to stdout.

    Example:
        Table: bronze_customers | Field: customer_id | Total: 10000 | Duplicates: 10 | Pass Rate: 99.9%

    Args:
        metric: Computed uniqueness statistics.
    """
    line = (
        f"Table: {metric.table_name} | Field: {metric.field_name} | "
        f"Total: {metric.total_rows} | Duplicates: {metric.duplicate_rows} | "
        f"Pass Rate: {metric.pass_rate:.2f}%"
    )
    print(line)
    logger.info(line)


def check_customers_uniqueness(
    spark: SparkSession,
    customers_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, UniquenessMetric]:
    """
    Run uniqueness check on bronze_customers.customer_id.

    Args:
        spark: Active SparkSession.
        customers_df: Optional pre-loaded customers DataFrame.

    Returns:
        Tuple of (flagged DataFrame, uniqueness metric).
    """
    source_df = customers_df if customers_df is not None else load_bronze_table(
        spark, CUSTOMERS_TABLE
    )
    flagged_df = flag_uniqueness(source_df, CUSTOMER_PK)
    metric = compute_uniqueness_metric(flagged_df, CUSTOMERS_TABLE, CUSTOMER_PK)
    print_uniqueness_metric(metric)
    return flagged_df, metric


def check_orders_uniqueness(
    spark: SparkSession,
    orders_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, UniquenessMetric]:
    """
    Run uniqueness check on bronze_orders.order_id.

    Args:
        spark: Active SparkSession.
        orders_df: Optional pre-loaded orders DataFrame.

    Returns:
        Tuple of (flagged DataFrame, uniqueness metric).
    """
    source_df = orders_df if orders_df is not None else load_bronze_table(spark, ORDERS_TABLE)
    flagged_df = flag_uniqueness(source_df, ORDER_PK)
    metric = compute_uniqueness_metric(flagged_df, ORDERS_TABLE, ORDER_PK)
    print_uniqueness_metric(metric)
    return flagged_df, metric


def run_uniqueness_checks(spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Execute uniqueness checks on configured Bronze tables.

    All rows are kept; duplicates are flagged only.

    Args:
        spark: Active SparkSession.

    Returns:
        Dictionary mapping table name to flagged DataFrame.
    """
    print("=" * 72)
    print("Silver uniqueness check — starting")
    print("=" * 72)

    try:
        customers_flagged, _ = check_customers_uniqueness(spark)
        orders_flagged, _ = check_orders_uniqueness(spark)
    except Exception as exc:
        logger.error("Uniqueness check failed: %s", exc)
        print(f"[silver] ERROR uniqueness check failed: {exc}")
        raise

    print("=" * 72)
    print("Silver uniqueness check — complete (no rows deleted or filtered)")
    print("=" * 72)

    return {
        CUSTOMERS_TABLE: customers_flagged,
        ORDERS_TABLE: orders_flagged,
    }


def main() -> int:
    """
    Run uniqueness checks as a standalone Silver job.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        spark = get_spark()
        results = run_uniqueness_checks(spark)

        for table_name, flagged_df in results.items():
            bronze_count = spark.table(table_name).count()
            flagged_count = flagged_df.count()
            logger.info(
                "%s row count unchanged after flagging: %d",
                table_name,
                flagged_count,
            )
            if bronze_count != flagged_count:
                logger.warning(
                    "Row count mismatch for %s: bronze=%d flagged=%d",
                    table_name,
                    bronze_count,
                    flagged_count,
                )

        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
