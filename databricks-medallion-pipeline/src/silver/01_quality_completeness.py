"""
Silver layer: data quality check — completeness.

Reads Bronze Delta tables and flags rows with NULL values in critical fields.
Adds a quality_completeness column ("PASS" or "FAIL: NULL {field_name}") without
removing or filtering any rows.

Checks:
  - bronze_customers: email NOT NULL
  - bronze_orders: customer_id and product_id NOT NULL

Databricks Community Edition compatible.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, isNull, lit, when

logger = logging.getLogger(__name__)

APP_NAME = "silver_quality_completeness"

CUSTOMERS_TABLE = "bronze_customers"
ORDERS_TABLE = "bronze_orders"

CUSTOMER_COMPLETENESS_FIELD = "email"
ORDER_COMPLETENESS_FIELDS = ("customer_id", "product_id")


@dataclass
class CompletenessMetric:
    """Completeness statistics for one table and field."""

    table_name: str
    field_name: str
    total_rows: int
    null_count: int

    @property
    def pass_rate(self) -> float:
        """Return pass rate as a percentage (0–100)."""
        if self.total_rows == 0:
            return 100.0
        return (self.total_rows - self.null_count) / self.total_rows * 100.0


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
        table_name: Bronze table name (e.g. bronze_customers).

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


def compute_field_metric(
    df: DataFrame,
    table_name: str,
    field_name: str,
) -> CompletenessMetric:
    """
    Compute null counts and pass rate for a single field.

    Uses aggregate count() so metrics are computed without filtering rows out.

    Args:
        df: Input DataFrame (unchanged — no rows filtered).
        table_name: Logical table name for reporting.
        field_name: Column to evaluate for NULL values.

    Returns:
        CompletenessMetric with totals and null count.
    """
    summary = df.agg(
        count(lit(1)).alias("total_rows"),
        count(when(isNull(col(field_name)), lit(1))).alias("null_count"),
    ).collect()[0]

    return CompletenessMetric(
        table_name=table_name,
        field_name=field_name,
        total_rows=int(summary["total_rows"]),
        null_count=int(summary["null_count"]),
    )


def print_completeness_metric(metric: CompletenessMetric) -> None:
    """
    Print one completeness metric line to stdout.

    Example:
        Table: bronze_customers | Field: email | Total: 10000 | Nulls: 50 | Pass Rate: 99.5%

    Args:
        metric: Computed completeness statistics.
    """
    line = (
        f"Table: {metric.table_name} | Field: {metric.field_name} | "
        f"Total: {metric.total_rows} | Nulls: {metric.null_count} | "
        f"Pass Rate: {metric.pass_rate:.1f}%"
    )
    print(line)
    logger.info(line)


def flag_customers_completeness(customers_df: DataFrame) -> DataFrame:
    """
    Add quality_completeness flag for bronze_customers.

    Rule: email must NOT be NULL.

    Args:
        customers_df: Bronze customers DataFrame.

    Returns:
        Same rows with quality_completeness column added (no rows removed).
    """
    return customers_df.withColumn(
        "quality_completeness",
        when(
            isNull(col(CUSTOMER_COMPLETENESS_FIELD)),
            lit(f"FAIL: NULL {CUSTOMER_COMPLETENESS_FIELD}"),
        ).otherwise(lit("PASS")),
    )


def flag_orders_completeness(orders_df: DataFrame) -> DataFrame:
    """
    Add quality_completeness flag for bronze_orders.

    Rules: customer_id and product_id must NOT be NULL.
    When multiple fields are NULL, customer_id failure takes precedence in the flag text.

    Args:
        orders_df: Bronze orders DataFrame.

    Returns:
        Same rows with quality_completeness column added (no rows removed).
    """
    return orders_df.withColumn(
        "quality_completeness",
        when(
            isNull(col("customer_id")),
            lit("FAIL: NULL customer_id"),
        )
        .when(
            isNull(col("product_id")),
            lit("FAIL: NULL product_id"),
        )
        .otherwise(lit("PASS")),
    )


def check_customers_completeness(
    spark: SparkSession,
    customers_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, CompletenessMetric]:
    """
    Run completeness check on bronze_customers and return flagged DataFrame.

    Args:
        spark: Active SparkSession.
        customers_df: Optional pre-loaded customers DataFrame.

    Returns:
        Tuple of (flagged DataFrame, email completeness metric).
    """
    source_df = customers_df if customers_df is not None else load_bronze_table(
        spark, CUSTOMERS_TABLE
    )
    metric = compute_field_metric(source_df, CUSTOMERS_TABLE, CUSTOMER_COMPLETENESS_FIELD)
    flagged_df = flag_customers_completeness(source_df)
    print_completeness_metric(metric)
    return flagged_df, metric


def check_orders_completeness(
    spark: SparkSession,
    orders_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, Dict[str, CompletenessMetric]]:
    """
    Run completeness checks on bronze_orders and return flagged DataFrame.

    Args:
        spark: Active SparkSession.
        orders_df: Optional pre-loaded orders DataFrame.

    Returns:
        Tuple of (flagged DataFrame, dict of field metrics keyed by field name).
    """
    source_df = orders_df if orders_df is not None else load_bronze_table(spark, ORDERS_TABLE)

    metrics = {
        field: compute_field_metric(source_df, ORDERS_TABLE, field)
        for field in ORDER_COMPLETENESS_FIELDS
    }
    flagged_df = flag_orders_completeness(source_df)

    for field in ORDER_COMPLETENESS_FIELDS:
        print_completeness_metric(metrics[field])

    return flagged_df, metrics


def run_completeness_checks(
    spark: SparkSession,
) -> Dict[str, DataFrame]:
    """
    Execute completeness checks on all configured Bronze tables.

    Does not delete or filter rows — only adds quality_completeness column.

    Args:
        spark: Active SparkSession.

    Returns:
        Dictionary mapping table name to flagged DataFrame:
        {"bronze_customers": df, "bronze_orders": df}
    """
    print("=" * 72)
    print("Silver completeness check — starting")
    print("=" * 72)

    try:
        customers_flagged, _ = check_customers_completeness(spark)
        orders_flagged, _ = check_orders_completeness(spark)
    except Exception as exc:
        logger.error("Completeness check failed: %s", exc)
        print(f"[silver] ERROR completeness check failed: {exc}")
        raise

    print("=" * 72)
    print("Silver completeness check — complete (no rows deleted or filtered)")
    print("=" * 72)

    return {
        CUSTOMERS_TABLE: customers_flagged,
        ORDERS_TABLE: orders_flagged,
    }


def main() -> int:
    """
    Run completeness checks as a standalone Silver job.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        spark = get_spark()
        results = run_completeness_checks(spark)

        # Verify row counts unchanged — flag only, never filter
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
