"""
Gold layer: create gold_customer_segmentation aggregation table.

PySpark equivalent of 04_customer_segmentation.sql for Databricks notebook execution.
Classifies customers from gold_revenue_by_customer into behavioral segments and
aggregates segment-level metrics.

Business question: How are customers distributed across behavioral segments?
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, lit, max as spark_max, round as spark_round, sum as spark_sum, when

logger = logging.getLogger(__name__)

APP_NAME = "gold_customer_segmentation"

GOLD_REVENUE_BY_CUSTOMER = "gold_revenue_by_customer"
GOLD_TABLE = "gold_customer_segmentation"


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


def classify_customer_segment(customer_metrics: DataFrame) -> DataFrame:
    """
    Assign each customer a segment_type based on revenue, order count, and recency.

    Priority (first match wins):
      1. Inactive:   days_since_last_order > 180
      2. High-Value: total_revenue > 10000 OR total_orders > 20
      3. One-Time:   total_orders = 1
      4. Repeat:     total_orders between 5 and 20 inclusive
      5. Regular:    all others

    Args:
        customer_metrics: Per-customer metrics from gold_revenue_by_customer.

    Returns:
        DataFrame with customer_id, total_orders, total_revenue, segment_type.
    """
    return customer_metrics.select(
        col("customer_id"),
        col("total_orders"),
        col("total_revenue"),
        when(col("days_since_last_order") > lit(180), lit("Inactive"))
        .when(
            (col("total_revenue") > lit(10000)) | (col("total_orders") > lit(20)),
            lit("High-Value"),
        )
        .when(col("total_orders") == lit(1), lit("One-Time"))
        .when(
            (col("total_orders") >= lit(5)) & (col("total_orders") <= lit(20)),
            lit("Repeat"),
        )
        .otherwise(lit("Regular"))
        .alias("segment_type"),
    )


def build_gold_customer_segmentation(spark: SparkSession) -> DataFrame:
    """
    Build segment-level summary metrics from classified customers.

    Args:
        spark: Active SparkSession.

    Returns:
        Aggregated DataFrame ordered by customer_count descending.
    """
    customer_metrics = spark.table(GOLD_REVENUE_BY_CUSTOMER).select(
        "customer_id",
        "total_orders",
        "total_revenue",
        "last_order_date",
        "days_since_last_order",
    )

    classified = classify_customer_segment(customer_metrics)
    total_customers = classified.count()

    gold_df = (
        classified.groupBy("segment_type")
        .agg(
            count(lit(1)).alias("customer_count"),
            avg("total_revenue").alias("avg_revenue"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_orders").alias("avg_orders"),
        )
        .withColumn(
            "pct_of_total_customers",
            spark_round((col("customer_count") / lit(total_customers)) * lit(100), 2),
        )
        .select(
            "segment_type",
            "customer_count",
            "avg_revenue",
            "total_revenue",
            "avg_orders",
            "pct_of_total_customers",
        )
        .orderBy(col("customer_count").desc())
    )

    return gold_df


def create_gold_customer_segmentation(spark: Optional[SparkSession] = None) -> int:
    """
    Materialize gold_customer_segmentation as a Delta table.

    Args:
        spark: Optional SparkSession (creates one if omitted).

    Returns:
        Number of segment rows written (typically 5 or fewer).
    """
    spark = spark or get_spark()
    start = time.perf_counter()

    try:
        logger.info("Building %s from %s", GOLD_TABLE, GOLD_REVENUE_BY_CUSTOMER)
        gold_df = build_gold_customer_segmentation(spark)
        row_count = gold_df.count()

        gold_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(GOLD_TABLE)

        elapsed = time.perf_counter() - start
        logger.info("Wrote %d rows to %s in %.2fs", row_count, GOLD_TABLE, elapsed)
        print(f"[gold] {GOLD_TABLE}: wrote {row_count:,} rows ({elapsed:.2f}s)")
        return row_count

    except Exception as exc:
        logger.error("Failed to create %s: %s", GOLD_TABLE, exc)
        print(f"[gold] ERROR creating {GOLD_TABLE}: {exc}")
        raise


def main() -> int:
    """
    Run gold_customer_segmentation creation as a standalone job.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        create_gold_customer_segmentation()
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
