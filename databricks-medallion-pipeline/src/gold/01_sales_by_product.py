"""
Gold layer: create gold_sales_by_product aggregation table.

PySpark equivalent of 01_sales_by_product.sql for Databricks notebook execution.
Aggregates PASS silver_orders joined to PASS silver_products by product.

Business question: Which products drive order volume and revenue?
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
    first,
    lit,
    round as spark_round,
    sum as spark_sum,
)

logger = logging.getLogger(__name__)

APP_NAME = "gold_sales_by_product"

SILVER_ORDERS = "silver_orders"
SILVER_PRODUCTS = "silver_products"
GOLD_TABLE = "gold_sales_by_product"


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


def build_gold_sales_by_product(spark: SparkSession) -> DataFrame:
    """
    Build the gold_sales_by_product aggregation from Silver PASS rows.

    Join logic:
      - Filter silver_orders and silver_products to quality_check_result = 'PASS'
      - Inner join on product_id
      - Group by product_id, product_name, category

    Metrics:
      - total_orders: distinct order_id count per product
      - total_quantity: sum of quantity sold
      - total_revenue: sum of total_amount
      - avg_order_value: average line total_amount
      - profit_margin: ((price - cost) / price) * 100 from product catalog

    Args:
        spark: Active SparkSession.

    Returns:
        Aggregated DataFrame ordered by total_revenue descending.
    """
    valid_orders = spark.table(SILVER_ORDERS).filter(
        col("quality_check_result") == lit("PASS")
    )
    valid_products = spark.table(SILVER_PRODUCTS).filter(
        col("quality_check_result") == lit("PASS")
    )

    joined = valid_orders.alias("o").join(
        valid_products.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "inner",
    )

    # price and cost are constant per product_id — first() matches SQL MAX(price/cost)
    gold_df = (
        joined.groupBy(
            col("p.product_id").alias("product_id"),
            col("p.product_name").alias("product_name"),
            col("p.category").alias("category"),
        )
        .agg(
            countDistinct(col("o.order_id")).alias("total_orders"),
            spark_sum(col("o.quantity")).alias("total_quantity"),
            spark_sum(col("o.total_amount")).alias("total_revenue"),
            avg(col("o.total_amount")).alias("avg_order_value"),
            first(col("p.price")).alias("_price"),
            first(col("p.cost")).alias("_cost"),
        )
        .withColumn(
            "profit_margin",
            spark_round(
                (
                    (col("_price").cast("double") - col("_cost").cast("double"))
                    / col("_price").cast("double")
                )
                * lit(100),
                2,
            ),
        )
        .drop("_price", "_cost")
        .orderBy(col("total_revenue").desc())
    )

    return gold_df


def create_gold_sales_by_product(spark: Optional[SparkSession] = None) -> int:
    """
    Materialize gold_sales_by_product as a Delta table.

    Args:
        spark: Optional SparkSession (creates one if omitted).

    Returns:
        Number of rows written to gold_sales_by_product.
    """
    spark = spark or get_spark()
    start = time.perf_counter()

    try:
        logger.info("Building %s from %s and %s", GOLD_TABLE, SILVER_ORDERS, SILVER_PRODUCTS)
        gold_df = build_gold_sales_by_product(spark)
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
    Run gold_sales_by_product creation as a standalone job.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        create_gold_sales_by_product()
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
