"""
Silver layer: data quality check — type and format validation.

Validates data types and business format rules on Bronze tables.
NULL values are skipped (handled by completeness checks) and do not fail
this validation. Adds quality_type_validation without removing rows.

Checks:
  - bronze_customers: email format, signup_date valid and not in future
  - bronze_orders: quantity positive integer, total_amount positive, order_date valid
  - bronze_products: price positive, cost positive and less than price

Databricks Community Edition compatible.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    count,
    current_date,
    floor,
    lit,
    to_date,
    when,
)
from pyspark.sql.types import DoubleType, IntegerType

logger = logging.getLogger(__name__)

APP_NAME = "silver_quality_type_validation"

CUSTOMERS_TABLE = "bronze_customers"
ORDERS_TABLE = "bronze_orders"
PRODUCTS_TABLE = "bronze_products"

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


@dataclass
class TypeValidationMetric:
    """Type validation statistics for one field-level rule."""

    table_name: str
    field_name: str
    rule_name: str
    evaluated_rows: int
    invalid_rows: int

    @property
    def pass_rate(self) -> float:
        """Return pass rate among evaluated (non-null) rows."""
        if self.evaluated_rows == 0:
            return 100.0
        return (self.evaluated_rows - self.invalid_rows) / self.evaluated_rows * 100.0


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


def is_valid_email(column_name: str = "email") -> Column:
    """Return True when email contains @ and . in a basic valid pattern."""
    email_col = col(column_name)
    return email_col.rlike(EMAIL_PATTERN)


def is_valid_past_date(column_name: str) -> Column:
    """Return True when value parses as a date on or before today."""
    parsed = to_date(col(column_name))
    return parsed.isNotNull() & (parsed <= current_date())


def is_positive_integer(column_name: str) -> Column:
    """Return True when value is a positive integer (> 0, no fractional part)."""
    numeric = col(column_name).cast(DoubleType())
    as_int = col(column_name).cast(IntegerType())
    return (
        numeric.isNotNull()
        & as_int.isNotNull()
        & (numeric > 0)
        & (floor(numeric) == numeric)
    )


def is_positive_decimal(column_name: str) -> Column:
    """Return True when value casts to a positive decimal."""
    numeric = col(column_name).cast(DoubleType())
    return numeric.isNotNull() & (numeric > 0)


def is_positive_price(column_name: str) -> Column:
    """Return True when price is a positive number."""
    numeric = col(column_name).cast(DoubleType())
    return numeric.isNotNull() & (numeric > 0)


def is_valid_cost_vs_price(cost_col: str = "cost", price_col: str = "price") -> Column:
    """Return True when cost is positive and strictly less than price."""
    cost = col(cost_col).cast(DoubleType())
    price = col(price_col).cast(DoubleType())
    return cost.isNotNull() & price.isNotNull() & (cost > 0) & (cost < price)


def compute_rule_metric(
    df: DataFrame,
    table_name: str,
    field_name: str,
    rule_name: str,
    is_valid_expr: Column,
) -> TypeValidationMetric:
    """
    Compute validation metrics for one rule, skipping NULL field values.

    Args:
        df: Source DataFrame.
        table_name: Table name for reporting.
        field_name: Column under test.
        rule_name: Short rule description.
        is_valid_expr: Boolean column expression (True = valid).

    Returns:
        TypeValidationMetric for the rule.
    """
    field = col(field_name)
    summary = df.agg(
        count(when(field.isNotNull(), lit(1))).alias("evaluated_rows"),
        count(
            when(field.isNotNull() & (~is_valid_expr), lit(1))
        ).alias("invalid_rows"),
    ).collect()[0]

    return TypeValidationMetric(
        table_name=table_name,
        field_name=field_name,
        rule_name=rule_name,
        evaluated_rows=int(summary["evaluated_rows"]),
        invalid_rows=int(summary["invalid_rows"]),
    )


def print_type_validation_metric(metric: TypeValidationMetric) -> None:
    """
    Print one type validation metric line.

    Example:
        Table: bronze_customers | Field: email | Rule: email format | Evaluated: 9950 | Invalid: 0 | Pass Rate: 100.0%
    """
    line = (
        f"Table: {metric.table_name} | Field: {metric.field_name} | "
        f"Rule: {metric.rule_name} | Evaluated: {metric.evaluated_rows} | "
        f"Invalid: {metric.invalid_rows} | Pass Rate: {metric.pass_rate:.2f}%"
    )
    print(line)
    logger.info(line)


def _apply_rule(
    result: Column,
    field_name: str,
    field_col: Column,
    valid_expr: Column,
    fail_reason: str,
) -> Column:
    """
    Chain one validation rule onto a when/otherwise Column expression.

    NULL field values are skipped. Only the first failure is kept when multiple
    rules fail on the same row.
    """
    fail_message = lit(f"FAIL: INVALID {field_name} - {fail_reason}")
    return when(
        field_col.isNotNull() & (~valid_expr) & (result == lit("PASS")),
        fail_message,
    ).otherwise(result)


def flag_customers_type_validation(customers_df: DataFrame) -> DataFrame:
    """
    Add quality_type_validation for bronze_customers.

    Args:
        customers_df: Bronze customers DataFrame.

    Returns:
        DataFrame with quality_type_validation column (all rows retained).
    """
    result = lit("PASS")

    result = _apply_rule(
        result,
        "email",
        col("email"),
        is_valid_email("email"),
        "invalid email format",
    )
    result = _apply_rule(
        result,
        "signup_date",
        col("signup_date"),
        is_valid_past_date("signup_date"),
        "invalid or future date",
    )

    return customers_df.withColumn("quality_type_validation", result)


def flag_orders_type_validation(orders_df: DataFrame) -> DataFrame:
    """
    Add quality_type_validation for bronze_orders.

    Args:
        orders_df: Bronze orders DataFrame.

    Returns:
        DataFrame with quality_type_validation column (all rows retained).
    """
    result = lit("PASS")

    result = _apply_rule(
        result,
        "quantity",
        col("quantity"),
        is_positive_integer("quantity"),
        "must be positive integer",
    )
    result = _apply_rule(
        result,
        "total_amount",
        col("total_amount"),
        is_positive_decimal("total_amount"),
        "must be positive decimal",
    )
    result = _apply_rule(
        result,
        "order_date",
        col("order_date"),
        is_valid_past_date("order_date"),
        "invalid or future date",
    )

    return orders_df.withColumn("quality_type_validation", result)


def flag_products_type_validation(products_df: DataFrame) -> DataFrame:
    """
    Add quality_type_validation for bronze_products.

    Args:
        products_df: Bronze products DataFrame.

    Returns:
        DataFrame with quality_type_validation column (all rows retained).
    """
    result = lit("PASS")

    result = _apply_rule(
        result,
        "price",
        col("price"),
        is_positive_price("price"),
        "must be positive",
    )

    # cost NULL is skipped; when present it must be positive and below price
    cost_valid = is_valid_cost_vs_price("cost", "price")
    result = when(
        col("cost").isNotNull()
        & (~cost_valid)
        & (result == lit("PASS")),
        lit("FAIL: INVALID cost - must be positive and less than price"),
    ).otherwise(result)

    return products_df.withColumn("quality_type_validation", result)


def evaluate_customer_metrics(customers_df: DataFrame) -> List[TypeValidationMetric]:
    """Compute and print metrics for all customer type validation rules."""
    rules = [
        ("email", "email format", is_valid_email("email")),
        ("signup_date", "valid date not in future", is_valid_past_date("signup_date")),
    ]
    metrics = [
        compute_rule_metric(customers_df, CUSTOMERS_TABLE, field, rule, expr)
        for field, rule, expr in rules
    ]
    for metric in metrics:
        print_type_validation_metric(metric)
    return metrics


def evaluate_order_metrics(orders_df: DataFrame) -> List[TypeValidationMetric]:
    """Compute and print metrics for all order type validation rules."""
    rules = [
        ("quantity", "positive integer", is_positive_integer("quantity")),
        ("total_amount", "positive decimal", is_positive_decimal("total_amount")),
        ("order_date", "valid date not in future", is_valid_past_date("order_date")),
    ]
    metrics = [
        compute_rule_metric(orders_df, ORDERS_TABLE, field, rule, expr)
        for field, rule, expr in rules
    ]
    for metric in metrics:
        print_type_validation_metric(metric)
    return metrics


def evaluate_product_metrics(products_df: DataFrame) -> List[TypeValidationMetric]:
    """Compute and print metrics for all product type validation rules."""
    price_metric = compute_rule_metric(
        products_df,
        PRODUCTS_TABLE,
        "price",
        "positive price",
        is_positive_price("price"),
    )
    print_type_validation_metric(price_metric)

    # cost rule: evaluated only where cost IS NOT NULL
    cost_valid = is_valid_cost_vs_price("cost", "price")
    cost_summary = products_df.agg(
        count(when(col("cost").isNotNull(), lit(1))).alias("evaluated_rows"),
        count(when(col("cost").isNotNull() & (~cost_valid), lit(1))).alias("invalid_rows"),
    ).collect()[0]
    cost_metric = TypeValidationMetric(
        table_name=PRODUCTS_TABLE,
        field_name="cost",
        rule_name="positive and less than price",
        evaluated_rows=int(cost_summary["evaluated_rows"]),
        invalid_rows=int(cost_summary["invalid_rows"]),
    )
    print_type_validation_metric(cost_metric)

    return [price_metric, cost_metric]


def check_customers_type_validation(
    spark: SparkSession,
    customers_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, List[TypeValidationMetric]]:
    """Run type validation on bronze_customers."""
    source_df = customers_df if customers_df is not None else load_bronze_table(
        spark, CUSTOMERS_TABLE
    )
    metrics = evaluate_customer_metrics(source_df)
    flagged_df = flag_customers_type_validation(source_df)
    return flagged_df, metrics


def check_orders_type_validation(
    spark: SparkSession,
    orders_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, List[TypeValidationMetric]]:
    """Run type validation on bronze_orders."""
    source_df = orders_df if orders_df is not None else load_bronze_table(spark, ORDERS_TABLE)
    metrics = evaluate_order_metrics(source_df)
    flagged_df = flag_orders_type_validation(source_df)
    return flagged_df, metrics


def check_products_type_validation(
    spark: SparkSession,
    products_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, List[TypeValidationMetric]]:
    """Run type validation on bronze_products."""
    source_df = products_df if products_df is not None else load_bronze_table(
        spark, PRODUCTS_TABLE
    )
    metrics = evaluate_product_metrics(source_df)
    flagged_df = flag_products_type_validation(source_df)
    return flagged_df, metrics


def run_type_validation_checks(spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Execute type validation on all configured Bronze tables.

    Args:
        spark: Active SparkSession.

    Returns:
        Dictionary of flagged DataFrames keyed by table name.
    """
    print("=" * 72)
    print("Silver type validation check — starting")
    print("=" * 72)

    try:
        customers_flagged, _ = check_customers_type_validation(spark)
        orders_flagged, _ = check_orders_type_validation(spark)
        products_flagged, _ = check_products_type_validation(spark)
    except Exception as exc:
        logger.error("Type validation check failed: %s", exc)
        print(f"[silver] ERROR type validation check failed: {exc}")
        raise

    print("=" * 72)
    print("Silver type validation check — complete (NULLs skipped, no rows filtered)")
    print("=" * 72)

    return {
        CUSTOMERS_TABLE: customers_flagged,
        ORDERS_TABLE: orders_flagged,
        PRODUCTS_TABLE: products_flagged,
    }


def main() -> int:
    """
    Run type validation checks as a standalone Silver job.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        spark = get_spark()
        results = run_type_validation_checks(spark)

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
