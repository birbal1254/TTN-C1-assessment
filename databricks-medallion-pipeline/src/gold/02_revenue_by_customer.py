"""
Gold layer: create gold_revenue_by_customer aggregation table.

PySpark equivalent of 02_revenue_by_customer.sql for Databricks notebook execution.
Aggregates PASS silver_orders joined to PASS silver_customers by customer.

Business question: Who are our highest-value customers and how recently did they purchase?
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    current_date,
    datediff,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
)

logger = logging.getLogger(__name__)

APP_NAME = "gold_revenue_by_customer"

SILVER_ORDERS = "silver_orders"
SILVER_CUSTOMERS = "silver_customers"
GOLD_TABLE = "gold_revenue_by_customer"


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


def build_gold_revenue_by_customer(spark: SparkSession) -> DataFrame:
    """
    Build the gold_revenue_by_customer aggregation from Silver PASS rows.

    Join logic:
      - Filter silver_orders and silver_customers to quality_check_result = 'PASS'
      - Inner join on customer_id
      - Group by customer_id, customer_name, customer_segment, country

    Metrics:
      - total_orders: distinct order_id count per customer
      - total_revenue: sum of total_amount
      - avg_order_value: average order line amount
      - first_order_date / last_order_date: min/max order_date
      - lifetime_value_actual: total_revenue from order history
      - days_since_last_order: days from today to last_order_date

    Args:
        spark: Active SparkSession.

    Returns:
        Aggregated DataFrame ordered by total_revenue descending.
    """
    valid_orders = spark.table(SILVER_ORDERS).filter(
        col("quality_check_result") == lit("PASS")
    )
    valid_customers = spark.table(SILVER_CUSTOMERS).filter(
        col("quality_check_result") == lit("PASS")
    )

    joined = valid_orders.alias("o").join(
        valid_customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "inner",
    )

    aggregated = joined.groupBy(
        col("c.customer_id").alias("customer_id"),
        col("c.customer_name").alias("customer_name"),
        col("c.customer_segment").alias("customer_segment"),
        col("c.country").alias("country"),
    ).agg(
        countDistinct(col("o.order_id")).alias("total_orders"),
        spark_sum(col("o.total_amount")).alias("total_revenue"),
        avg(col("o.total_amount")).alias("avg_order_value"),
        spark_min(col("o.order_date")).alias("first_order_date"),
        spark_max(col("o.order_date")).alias("last_order_date"),
    )

    gold_df = (
        aggregated
        # Lifetime value calculated from actual order revenue
        .withColumn("lifetime_value_actual", col("total_revenue"))
        .withColumn(
            "days_since_last_order",
            datediff(current_date(), col("last_order_date").cast("date")),
        )
        .select(
            "customer_id",
            "customer_name",
            "customer_segment",
            "country",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "first_order_date",
            "last_order_date",
            "lifetime_value_actual",
            "days_since_last_order",
        )
        .orderBy(col("total_revenue").desc())
    )

    return gold_df


def create_gold_revenue_by_customer(spark: Optional[SparkSession] = None) -> int:
    """
    Materialize gold_revenue_by_customer as a Delta table.

    Args:
        spark: Optional SparkSession (creates one if omitted).

    Returns:
        Number of rows written to gold_revenue_by_customer.
    """
    spark = spark or get_spark()
    start = time.perf_counter()

    try:
        logger.info(
            "Building %s from %s and %s",
            GOLD_TABLE,
            SILVER_ORDERS,
            SILVER_CUSTOMERS,
        )
        gold_df = build_gold_revenue_by_customer(spark)
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
    Run gold_revenue_by_customer creation as a standalone job.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        create_gold_revenue_by_customer()
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
