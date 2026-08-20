"""
Silver layer: data quality check — referential integrity.

Validates foreign key relationships between Bronze orders and parent tables.
Orphan orders (customer_id or product_id not found in parent) are flagged but
never removed. NULL foreign keys are skipped (handled by completeness checks).

Checks:
  - orders.customer_id must exist in bronze_customers.customer_id
  - orders.product_id must exist in bronze_products.product_id

Uses left anti join logic to detect orphan keys.

Databricks Community Edition compatible.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, count, lit, when

logger = logging.getLogger(__name__)

APP_NAME = "silver_quality_referential_integrity"

CUSTOMERS_TABLE = "bronze_customers"
ORDERS_TABLE = "bronze_orders"
PRODUCTS_TABLE = "bronze_products"


@dataclass
class ReferentialIntegrityMetric:
    """Referential integrity statistics for one foreign key check."""

    check_name: str
    total_rows: int
    orphan_rows: int

    @property
    def pass_rate(self) -> float:
        """Return pass rate as a percentage (0–100) across all order rows."""
        if self.total_rows == 0:
            return 100.0
        return (self.total_rows - self.orphan_rows) / self.total_rows * 100.0


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


def distinct_parent_keys(parent_df: DataFrame, key_column: str) -> DataFrame:
    """
    Return distinct parent primary keys for referential lookups.

    Args:
        parent_df: Parent Bronze table DataFrame.
        key_column: Primary key column name.

    Returns:
        Distinct key DataFrame with one column.
    """
    return parent_df.select(col(key_column)).distinct()


def count_orphan_rows_left_anti(
    orders_df: DataFrame,
    parent_keys_df: DataFrame,
    foreign_key: str,
    parent_key: str,
) -> int:
    """
    Count orphan order rows using a left anti join (FK not in parent).

    NULL foreign keys are excluded before the anti join because completeness
    checks own NULL handling.

    Args:
        orders_df: Orders DataFrame.
        parent_keys_df: Distinct parent key DataFrame.
        foreign_key: Foreign key column on orders.
        parent_key: Primary key column on parent.

    Returns:
        Number of orphan rows with non-null foreign key.
    """
    non_null_orders = orders_df.filter(col(foreign_key).isNotNull())
    orphan_rows = non_null_orders.join(
        parent_keys_df,
        non_null_orders[foreign_key] == parent_keys_df[parent_key],
        "left_anti",
    )
    return orphan_rows.count()


def compute_referential_metric(
    orders_df: DataFrame,
    parent_keys_df: DataFrame,
    check_name: str,
    foreign_key: str,
    parent_key: str,
) -> ReferentialIntegrityMetric:
    """
    Compute orphan counts and pass rate for one FK relationship.

    Args:
        orders_df: Orders DataFrame.
        parent_keys_df: Distinct parent keys.
        check_name: Label for reporting.
        foreign_key: Orders FK column.
        parent_key: Parent PK column.

    Returns:
        ReferentialIntegrityMetric with totals and orphan count.
    """
    total_rows = orders_df.count()
    orphan_rows = count_orphan_rows_left_anti(
        orders_df,
        parent_keys_df,
        foreign_key,
        parent_key,
    )
    return ReferentialIntegrityMetric(
        check_name=check_name,
        total_rows=total_rows,
        orphan_rows=orphan_rows,
    )


def print_referential_metric(metric: ReferentialIntegrityMetric) -> None:
    """
    Print one referential integrity metric line.

    Example:
        Check: customer_id referential | Total: 100000 | Orphans: 50 | Pass Rate: 99.95%
    """
    line = (
        f"Check: {metric.check_name} | Total: {metric.total_rows} | "
        f"Orphans: {metric.orphan_rows} | Pass Rate: {metric.pass_rate:.2f}%"
    )
    print(line)
    logger.info(line)


def flag_orders_referential_integrity(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """
    Flag orphan foreign keys on orders using left joins to parent key sets.

    Left anti join logic:
      - customer_id NOT IN customers → FAIL: ORPHAN customer_id
      - product_id NOT IN products   → FAIL: ORPHAN product_id

    NULL FK values are skipped (quality_referential_integrity remains PASS).

    Args:
        orders_df: Bronze orders DataFrame.
        customers_df: Bronze customers DataFrame.
        products_df: Bronze products DataFrame.

    Returns:
        Orders DataFrame with quality_referential_integrity column (all rows kept).
    """
    customer_keys = broadcast(distinct_parent_keys(customers_df, "customer_id")).withColumnRenamed(
        "customer_id",
        "_ref_customer_id",
    )
    product_keys = broadcast(distinct_parent_keys(products_df, "product_id")).withColumnRenamed(
        "product_id",
        "_ref_product_id",
    )

    enriched = (
        orders_df.join(
            customer_keys,
            col("customer_id") == col("_ref_customer_id"),
            "left",
        )
        .join(
            product_keys,
            col("product_id") == col("_ref_product_id"),
            "left",
        )
    )

    customer_orphan = col("customer_id").isNotNull() & col("_ref_customer_id").isNull()
    product_orphan = col("product_id").isNotNull() & col("_ref_product_id").isNull()

    flagged = enriched.withColumn(
        "quality_referential_integrity",
        when(customer_orphan, lit("FAIL: ORPHAN customer_id"))
        .when(product_orphan, lit("FAIL: ORPHAN product_id"))
        .otherwise(lit("PASS")),
    ).drop("_ref_customer_id", "_ref_product_id")

    return flagged


def check_orders_referential_integrity(
    spark: SparkSession,
    orders_df: Optional[DataFrame] = None,
    customers_df: Optional[DataFrame] = None,
    products_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, List[ReferentialIntegrityMetric]]:
    """
    Run referential integrity checks on bronze_orders.

    Args:
        spark: Active SparkSession.
        orders_df: Optional pre-loaded orders DataFrame.
        customers_df: Optional pre-loaded customers DataFrame.
        products_df: Optional pre-loaded products DataFrame.

    Returns:
        Tuple of (flagged orders DataFrame, list of metrics).
    """
    orders = orders_df if orders_df is not None else load_bronze_table(spark, ORDERS_TABLE)
    customers = (
        customers_df if customers_df is not None else load_bronze_table(spark, CUSTOMERS_TABLE)
    )
    products = products_df if products_df is not None else load_bronze_table(spark, PRODUCTS_TABLE)

    customer_keys = distinct_parent_keys(customers, "customer_id")
    product_keys = distinct_parent_keys(products, "product_id")

    metrics = [
        compute_referential_metric(
            orders,
            customer_keys,
            "customer_id referential",
            "customer_id",
            "customer_id",
        ),
        compute_referential_metric(
            orders,
            product_keys,
            "product_id referential",
            "product_id",
            "product_id",
        ),
    ]

    for metric in metrics:
        print_referential_metric(metric)

    flagged_orders = flag_orders_referential_integrity(orders, customers, products)
    return flagged_orders, metrics


def run_referential_integrity_checks(spark: SparkSession) -> DataFrame:
    """
    Execute referential integrity validation on bronze_orders.

    Args:
        spark: Active SparkSession.

    Returns:
        Flagged orders DataFrame with quality_referential_integrity column.
    """
    print("=" * 72)
    print("Silver referential integrity check — starting")
    print("=" * 72)

    try:
        flagged_orders, _ = check_orders_referential_integrity(spark)
    except Exception as exc:
        logger.error("Referential integrity check failed: %s", exc)
        print(f"[silver] ERROR referential integrity check failed: {exc}")
        raise

    print("=" * 72)
    print("Silver referential integrity check — complete (NULL FKs skipped, no rows filtered)")
    print("=" * 72)

    return flagged_orders


def main() -> int:
    """
    Run referential integrity checks as a standalone Silver job.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        spark = get_spark()
        flagged_orders = run_referential_integrity_checks(spark)

        bronze_count = spark.table(ORDERS_TABLE).count()
        flagged_count = flagged_orders.count()
        logger.info(
            "%s row count unchanged after flagging: %d",
            ORDERS_TABLE,
            flagged_count,
        )
        if bronze_count != flagged_count:
            logger.warning(
                "Row count mismatch for %s: bronze=%d flagged=%d",
                ORDERS_TABLE,
                bronze_count,
                flagged_count,
            )

        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
