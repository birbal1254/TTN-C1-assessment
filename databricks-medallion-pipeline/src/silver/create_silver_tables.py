"""
Silver layer orchestrator: run quality checks and create Silver Delta tables.

Reads Bronze tables, applies completeness, uniqueness, type validation, and
referential integrity checks, combines results into quality_check_result,
writes silver_customers, silver_orders, silver_products, and persists
silver_quality_report.

Databricks Community Edition compatible.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, concat_ws, count, lit, when
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

if __name__ == "__main__" and str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger(__name__)

APP_NAME = "silver_create_tables"

BRONZE_CUSTOMERS = "bronze_customers"
BRONZE_ORDERS = "bronze_orders"
BRONZE_PRODUCTS = "bronze_products"

SILVER_CUSTOMERS = "silver_customers"
SILVER_ORDERS = "silver_orders"
SILVER_PRODUCTS = "silver_products"
SILVER_QUALITY_REPORT = "silver_quality_report"

QUALITY_REPORT_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("check_type", StringType(), False),
        StructField("total_rows", LongType(), False),
        StructField("passed", LongType(), False),
        StructField("failed", LongType(), False),
        StructField("pass_rate", DoubleType(), False),
    ]
)

CHECK_COLUMN_BY_TYPE = {
    "completeness": "quality_completeness",
    "uniqueness": "quality_uniqueness",
    "type_validation": "quality_type_validation",
    "referential_integrity": "quality_referential_integrity",
}


@dataclass
class StepTiming:
    """Wall-clock timing for one orchestrator step."""

    step_name: str
    elapsed_seconds: float


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


def load_module_function(module_filename: str, function_name: str) -> Callable[..., Any]:
    """
    Load a function from a numbered Silver quality script in this directory.

    Args:
        module_filename: Python file name (e.g. 01_quality_completeness.py).
        function_name: Callable name to import.

    Returns:
        Loaded function object.
    """
    module_path = Path(__file__).parent / module_filename
    spec = importlib.util.spec_from_file_location(module_filename, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, function_name)


def load_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Load a Bronze Delta table.

    Args:
        spark: Active SparkSession.
        table_name: Bronze table name.

    Returns:
        Bronze DataFrame.

    Raises:
        Exception: If the table cannot be loaded.
    """
    try:
        df = spark.table(table_name)
        logger.info("Loaded Bronze table %s", table_name)
        return df
    except Exception as exc:
        logger.error("Failed to load Bronze table %s: %s", table_name, exc)
        raise


def combine_quality_check_result(
    df: DataFrame,
    check_columns: List[str],
) -> DataFrame:
    """
    Merge individual quality flag columns into quality_check_result.

    Args:
        df: DataFrame containing quality_* flag columns.
        check_columns: Ordered list of flag column names to combine.

    Returns:
        DataFrame with quality_check_result column added.
    """
    present_checks = [name for name in check_columns if name in df.columns]

    if not present_checks:
        return df.withColumn("quality_check_result", lit("PASS"))

    all_pass_condition = col(present_checks[0]) == lit("PASS")
    for check_col in present_checks[1:]:
        all_pass_condition = all_pass_condition & (col(check_col) == lit("PASS"))

    failure_exprs = [
        when(col(check_col) != lit("PASS"), col(check_col)) for check_col in present_checks
    ]
    failure_list = concat_ws(", ", *failure_exprs)

    return df.withColumn(
        "quality_check_result",
        when(all_pass_condition, lit("PASS")).otherwise(concat(lit("FAIL: "), failure_list)),
    )


def compute_check_metrics(
    df: DataFrame,
    table_name: str,
    check_type: str,
    check_column: str,
) -> Optional[Dict[str, Any]]:
    """
    Compute passed/failed counts for one quality check column.

    Args:
        df: DataFrame with a quality flag column.
        table_name: Silver/Bronze table name for reporting.
        check_type: Check category label.
        check_column: Column holding PASS/FAIL values.

    Returns:
        Metric dictionary or None if the check column is absent.
    """
    if check_column not in df.columns:
        return None

    summary = df.agg(
        count(lit(1)).alias("total_rows"),
        count(when(col(check_column) == lit("PASS"), lit(1))).alias("passed"),
        count(when(col(check_column) != lit("PASS"), lit(1))).alias("failed"),
    ).collect()[0]

    total_rows = int(summary["total_rows"])
    passed = int(summary["passed"])
    failed = int(summary["failed"])
    pass_rate = round(passed / total_rows, 4) if total_rows else 1.0

    return {
        "table_name": table_name,
        "check_type": check_type,
        "total_rows": total_rows,
        "passed": passed,
        "failed": failed,
        "pass_rate": pass_rate,
    }


def build_quality_metrics_report(
    spark: SparkSession,
    table_frames: Dict[str, DataFrame],
) -> DataFrame:
    """
    Build a quality metrics report DataFrame from flagged Silver DataFrames.

    Args:
        spark: Active SparkSession.
        table_frames: Mapping of silver table name to flagged DataFrame.

    Returns:
        Report DataFrame with table_name, check_type, total_rows, passed, failed, pass_rate.
    """
    metric_rows: List[Dict[str, Any]] = []

    for table_name, df in table_frames.items():
        for check_type, check_column in CHECK_COLUMN_BY_TYPE.items():
            metric = compute_check_metrics(df, table_name, check_type, check_column)
            if metric is not None:
                metric_rows.append(metric)

        overall = compute_check_metrics(
            df,
            table_name,
            "overall",
            "quality_check_result",
        )
        if overall is not None:
            metric_rows.append(overall)

    return spark.createDataFrame(metric_rows, schema=QUALITY_REPORT_SCHEMA)


def write_delta_table(df: DataFrame, table_name: str) -> int:
    """
    Overwrite a Delta table and return written row count.

    Args:
        df: DataFrame to persist.
        table_name: Target Delta table name.

    Returns:
        Number of rows written.
    """
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        table_name
    )
    return df.count()


def print_quality_summary(report_df: DataFrame) -> None:
    """
    Print the full quality metrics report to stdout.

    Args:
        report_df: Quality metrics report DataFrame.
    """
    print()
    print("=" * 88)
    print("Silver quality summary")
    print("=" * 88)
    print(
        f"{'table_name':<22} {'check_type':<22} {'total_rows':>12} "
        f"{'passed':>10} {'failed':>10} {'pass_rate':>10}"
    )
    print("-" * 88)

    rows = report_df.orderBy("table_name", "check_type").collect()
    for row in rows:
        print(
            f"{row['table_name']:<22} {row['check_type']:<22} {row['total_rows']:>12,} "
            f"{row['passed']:>10,} {row['failed']:>10,} {row['pass_rate']:>10.4f}"
        )

    print("=" * 88)


def build_silver_customers(
    customers_df: DataFrame,
    flag_customers_completeness: Callable[[DataFrame], DataFrame],
    flag_uniqueness: Callable[..., DataFrame],
    flag_customers_type_validation: Callable[[DataFrame], DataFrame],
) -> DataFrame:
    """
    Apply all relevant quality checks to customers and combine flags.

    Args:
        customers_df: Bronze customers DataFrame.
        flag_customers_completeness: Completeness flag function.
        flag_uniqueness: Uniqueness flag function.
        flag_customers_type_validation: Type validation flag function.

    Returns:
        Silver-ready customers DataFrame with quality_check_result.
    """
    flagged = flag_customers_completeness(customers_df)
    flagged = flag_uniqueness(flagged, "customer_id")
    flagged = flag_customers_type_validation(flagged)
    flagged = flagged.withColumn("quality_referential_integrity", lit("PASS"))
    return combine_quality_check_result(
        flagged,
        [
            "quality_completeness",
            "quality_uniqueness",
            "quality_type_validation",
            "quality_referential_integrity",
        ],
    )


def build_silver_products(
    products_df: DataFrame,
    flag_uniqueness: Callable[..., DataFrame],
    flag_products_type_validation: Callable[[DataFrame], DataFrame],
) -> DataFrame:
    """
    Apply all relevant quality checks to products and combine flags.

    Args:
        products_df: Bronze products DataFrame.
        flag_uniqueness: Uniqueness flag function.
        flag_products_type_validation: Type validation flag function.

    Returns:
        Silver-ready products DataFrame with quality_check_result.
    """
    flagged = products_df.withColumn("quality_completeness", lit("PASS"))
    flagged = flag_uniqueness(flagged, "product_id")
    flagged = flag_products_type_validation(flagged)
    flagged = flagged.withColumn("quality_referential_integrity", lit("PASS"))
    return combine_quality_check_result(
        flagged,
        [
            "quality_completeness",
            "quality_uniqueness",
            "quality_type_validation",
            "quality_referential_integrity",
        ],
    )


def build_silver_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
    flag_orders_completeness: Callable[[DataFrame], DataFrame],
    flag_uniqueness: Callable[..., DataFrame],
    flag_orders_type_validation: Callable[[DataFrame], DataFrame],
    flag_orders_referential_integrity: Callable[..., DataFrame],
) -> DataFrame:
    """
    Apply all quality checks to orders and combine flags.

    Args:
        orders_df: Bronze orders DataFrame.
        customers_df: Bronze customers DataFrame (for referential integrity).
        products_df: Bronze products DataFrame (for referential integrity).
        flag_orders_completeness: Completeness flag function.
        flag_uniqueness: Uniqueness flag function.
        flag_orders_type_validation: Type validation flag function.
        flag_orders_referential_integrity: Referential integrity flag function.

    Returns:
        Silver-ready orders DataFrame with quality_check_result.
    """
    flagged = flag_orders_completeness(orders_df)
    flagged = flag_uniqueness(flagged, "order_id")
    flagged = flag_orders_type_validation(flagged)
    flagged = flag_orders_referential_integrity(flagged, customers_df, products_df)
    return combine_quality_check_result(
        flagged,
        [
            "quality_completeness",
            "quality_uniqueness",
            "quality_type_validation",
            "quality_referential_integrity",
        ],
    )


def create_silver_tables(spark: Optional[SparkSession] = None) -> Dict[str, DataFrame]:
    """
    Orchestrate Silver layer: quality checks, table writes, and quality report.

    Steps:
      1. Read Bronze tables
      2. Run all quality checks and combine quality_check_result
      3. Write silver_customers, silver_orders, silver_products
      4. Build and save silver_quality_report

    Args:
        spark: Optional SparkSession (creates one if omitted).

    Returns:
        Dictionary of Silver table names to DataFrames.
    """
    timings: List[StepTiming] = []
    pipeline_start = time.perf_counter()

    spark = spark or get_spark()

    print("=" * 88)
    print("Silver layer orchestrator — starting")
    print("=" * 88)

    try:
        step_start = time.perf_counter()
        bronze_customers = load_bronze_table(spark, BRONZE_CUSTOMERS)
        bronze_orders = load_bronze_table(spark, BRONZE_ORDERS)
        bronze_products = load_bronze_table(spark, BRONZE_PRODUCTS)
        timings.append(StepTiming("Load Bronze tables", time.perf_counter() - step_start))
        logger.info("Loaded Bronze tables in %.2fs", timings[-1].elapsed_seconds)

        step_start = time.perf_counter()
        flag_customers_completeness = load_module_function(
            "01_quality_completeness.py",
            "flag_customers_completeness",
        )
        flag_orders_completeness = load_module_function(
            "01_quality_completeness.py",
            "flag_orders_completeness",
        )
        flag_uniqueness = load_module_function("02_quality_uniqueness.py", "flag_uniqueness")
        flag_customers_type_validation = load_module_function(
            "03_quality_type_validation.py",
            "flag_customers_type_validation",
        )
        flag_orders_type_validation = load_module_function(
            "03_quality_type_validation.py",
            "flag_orders_type_validation",
        )
        flag_products_type_validation = load_module_function(
            "03_quality_type_validation.py",
            "flag_products_type_validation",
        )
        flag_orders_referential_integrity = load_module_function(
            "04_quality_referential_integrity.py",
            "flag_orders_referential_integrity",
        )
        timings.append(StepTiming("Load quality modules", time.perf_counter() - step_start))

        step_start = time.perf_counter()
        silver_customers_df = build_silver_customers(
            bronze_customers,
            flag_customers_completeness,
            flag_uniqueness,
            flag_customers_type_validation,
        )
        silver_products_df = build_silver_products(
            bronze_products,
            flag_uniqueness,
            flag_products_type_validation,
        )
        silver_orders_df = build_silver_orders(
            bronze_orders,
            bronze_customers,
            bronze_products,
            flag_orders_completeness,
            flag_uniqueness,
            flag_orders_type_validation,
            flag_orders_referential_integrity,
        )
        timings.append(StepTiming("Run quality checks", time.perf_counter() - step_start))
        logger.info("Quality checks completed in %.2fs", timings[-1].elapsed_seconds)

        bronze_customer_count = bronze_customers.count()
        bronze_order_count = bronze_orders.count()
        bronze_product_count = bronze_products.count()

        if bronze_customer_count != silver_customers_df.count():
            raise ValueError(
                f"Row count mismatch for customers: bronze={bronze_customer_count}, "
                f"silver={silver_customers_df.count()}"
            )
        if bronze_order_count != silver_orders_df.count():
            raise ValueError(
                f"Row count mismatch for orders: bronze={bronze_order_count}, "
                f"silver={silver_orders_df.count()}"
            )
        if bronze_product_count != silver_products_df.count():
            raise ValueError(
                f"Row count mismatch for products: bronze={bronze_product_count}, "
                f"silver={silver_products_df.count()}"
            )

        step_start = time.perf_counter()
        customers_written = write_delta_table(silver_customers_df, SILVER_CUSTOMERS)
        products_written = write_delta_table(silver_products_df, SILVER_PRODUCTS)
        orders_written = write_delta_table(silver_orders_df, SILVER_ORDERS)
        timings.append(StepTiming("Write Silver tables", time.perf_counter() - step_start))
        print(
            f"[silver] Wrote {customers_written:,} → {SILVER_CUSTOMERS}, "
            f"{products_written:,} → {SILVER_PRODUCTS}, "
            f"{orders_written:,} → {SILVER_ORDERS}"
        )

        step_start = time.perf_counter()
        silver_frames = {
            SILVER_CUSTOMERS: silver_customers_df,
            SILVER_ORDERS: silver_orders_df,
            SILVER_PRODUCTS: silver_products_df,
        }
        quality_report_df = build_quality_metrics_report(spark, silver_frames)
        report_rows = write_delta_table(quality_report_df, SILVER_QUALITY_REPORT)
        timings.append(StepTiming("Write quality report", time.perf_counter() - step_start))
        print(f"[silver] Wrote {report_rows} metric rows → {SILVER_QUALITY_REPORT}")

        print_quality_summary(quality_report_df)

        total_elapsed = time.perf_counter() - pipeline_start
        print()
        print("Step timings:")
        for timing in timings:
            print(f"  - {timing.step_name}: {timing.elapsed_seconds:.2f}s")
        print(f"Total Silver pipeline time: {total_elapsed:.2f}s")
        print("=" * 88)

        return silver_frames

    except Exception as exc:
        logger.exception("Silver orchestrator failed: %s", exc)
        print(f"[silver] ERROR orchestrator failed: {exc}")
        raise


def main() -> int:
    """
    Run the Silver layer orchestrator.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        create_silver_tables()
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
