"""
Gold layer orchestrator: build all Gold aggregation Delta tables from Silver PASS data.

Creates:
  - gold_sales_by_product
  - gold_revenue_by_customer
  - gold_customer_segmentation (depends on gold_revenue_by_customer)

Validates row counts, key-column nulls, prints sample rows, and reports timing.
Continues remaining jobs if one table build fails.

Databricks Community Edition compatible.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from dataclasses import dataclass, field
from functools import reduce
from operator import or_
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__" and str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger(__name__)

APP_NAME = "gold_create_tables"

SILVER_TABLES = ("silver_customers", "silver_orders", "silver_products")


@dataclass
class GoldJobResult:
    """Outcome of one Gold table build job."""

    table_name: str
    success: bool
    row_count: int = 0
    elapsed_seconds: float = 0.0
    error_message: Optional[str] = None
    validation_passed: bool = False


@dataclass
class GoldJob:
    """Definition of one Gold table creation step."""

    table_name: str
    module_filename: str
    create_function_name: str
    key_columns: List[str] = field(default_factory=list)


GOLD_JOBS: List[GoldJob] = [
    GoldJob(
        table_name="gold_sales_by_product",
        module_filename="01_sales_by_product.py",
        create_function_name="create_gold_sales_by_product",
        key_columns=["product_id", "product_name", "category", "total_revenue"],
    ),
    GoldJob(
        table_name="gold_revenue_by_customer",
        module_filename="02_revenue_by_customer.py",
        create_function_name="create_gold_revenue_by_customer",
        key_columns=["customer_id", "customer_name", "total_revenue"],
    ),
    GoldJob(
        table_name="gold_customer_segmentation",
        module_filename="04_customer_segmentation.py",
        create_function_name="create_gold_customer_segmentation",
        key_columns=["segment_type", "customer_count", "total_revenue"],
    ),
]


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


def load_create_function(module_filename: str, function_name: str) -> Callable[..., Any]:
    """
    Load a Gold table create function from a numbered script in this directory.

    Args:
        module_filename: Python file name (e.g. 01_sales_by_product.py).
        function_name: Callable to import.

    Returns:
        Loaded create function.
    """
    module_path = Path(__file__).parent / module_filename
    spec = importlib.util.spec_from_file_location(module_filename, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, function_name)


def log_silver_pass_summary(spark: SparkSession) -> None:
    """
    Log PASS row counts from Silver source tables used by Gold aggregations.

    Args:
        spark: Active SparkSession.
    """
    print("Silver PASS row counts (Gold inputs):")
    for table_name in SILVER_TABLES:
        try:
            pass_count = (
                spark.table(table_name)
                .filter(col("quality_check_result") == "PASS")
                .count()
            )
            total_count = spark.table(table_name).count()
            print(f"  {table_name}: {pass_count:,} PASS / {total_count:,} total")
            logger.info(
                "%s PASS rows: %d of %d",
                table_name,
                pass_count,
                total_count,
            )
        except Exception as exc:
            logger.warning("Could not summarize %s: %s", table_name, exc)
            print(f"  {table_name}: unavailable ({exc})")


def validate_gold_table(
    spark: SparkSession,
    table_name: str,
    key_columns: List[str],
) -> int:
    """
    Validate a Gold Delta table after creation.

    Checks:
      - Row count > 0
      - No NULL values in key columns

    Args:
        spark: Active SparkSession.
        table_name: Gold table to validate.
        key_columns: Columns that must be non-null.

    Returns:
        Row count of the validated table.

    Raises:
        ValueError: If validation fails.
    """
    df = spark.table(table_name)
    row_count = df.count()

    if row_count <= 0:
        raise ValueError(f"{table_name} has no rows (row_count={row_count})")

    missing_columns = [name for name in key_columns if name not in df.columns]
    if missing_columns:
        raise ValueError(f"{table_name} missing expected columns: {missing_columns}")

    null_condition = reduce(or_, (col(name).isNull() for name in key_columns))
    null_rows = df.filter(null_condition).count()
    if null_rows > 0:
        raise ValueError(
            f"{table_name} has {null_rows} rows with NULL values in key columns "
            f"{key_columns}"
        )

    logger.info(
        "Validation passed for %s: %d rows, no NULLs in %s",
        table_name,
        row_count,
        key_columns,
    )
    return row_count


def print_sample_rows(spark: SparkSession, table_name: str, limit: int = 5) -> None:
    """
    Print the first N rows of a Gold table to stdout.

    Args:
        spark: Active SparkSession.
        table_name: Gold table name.
        limit: Number of sample rows to display.
    """
    print(f"Sample rows from {table_name} (first {limit}):")
    spark.table(table_name).show(limit, truncate=False)


def run_gold_job(spark: SparkSession, job: GoldJob) -> GoldJobResult:
    """
    Execute one Gold table build, validate, and print sample data.

    Args:
        spark: Active SparkSession.
        job: Gold job definition.

    Returns:
        GoldJobResult with success/failure details.
    """
    start = time.perf_counter()
    print("-" * 72)
    print(f"Building {job.table_name} ...")

    try:
        create_fn = load_create_function(job.module_filename, job.create_function_name)
        create_fn(spark)

        row_count = validate_gold_table(spark, job.table_name, job.key_columns)
        print_sample_rows(spark, job.table_name, limit=5)

        elapsed = time.perf_counter() - start
        print(f"[gold] {job.table_name}: SUCCESS — {row_count:,} rows ({elapsed:.2f}s)")

        return GoldJobResult(
            table_name=job.table_name,
            success=True,
            row_count=row_count,
            elapsed_seconds=elapsed,
            validation_passed=True,
        )

    except Exception as exc:
        elapsed = time.perf_counter() - start
        logger.exception("Gold job failed for %s", job.table_name)
        print(f"[gold] ERROR {job.table_name}: {exc}")
        return GoldJobResult(
            table_name=job.table_name,
            success=False,
            elapsed_seconds=elapsed,
            error_message=str(exc),
        )


def print_execution_summary(
    results: List[GoldJobResult],
    total_elapsed: float,
) -> None:
    """
    Print Gold orchestrator execution summary.

    Args:
        results: Per-table job outcomes.
        total_elapsed: Total pipeline wall-clock seconds.
    """
    print()
    print("=" * 72)
    print("Gold layer execution summary")
    print("=" * 72)
    print(f"{'Table':<30} {'Rows':>10} {'Time (s)':>10} {'Status':>10}")
    print("-" * 72)

    for result in results:
        status = "OK" if result.success else "FAILED"
        rows = f"{result.row_count:,}" if result.success else "-"
        print(
            f"{result.table_name:<30} {rows:>10} "
            f"{result.elapsed_seconds:>10.2f} {status:>10}"
        )
        if result.error_message:
            print(f"  error: {result.error_message}")

    successes = sum(1 for r in results if r.success)
    print("-" * 72)
    print(f"Tables succeeded: {successes}/{len(results)}")
    print(f"Total Gold pipeline time: {total_elapsed:.2f}s")
    print("=" * 72)


def create_gold_tables(spark: Optional[SparkSession] = None) -> Dict[str, GoldJobResult]:
    """
    Orchestrate creation of all Gold aggregation Delta tables.

    Steps:
      1. Summarize Silver PASS inputs
      2. Build each Gold table (continues on individual failures)
      3. Validate row counts and key-column nulls
      4. Print sample rows and execution summary

    Args:
        spark: Optional SparkSession (creates one if omitted).

    Returns:
        Mapping of table name to GoldJobResult.
    """
    spark = spark or get_spark()
    pipeline_start = time.perf_counter()
    results: List[GoldJobResult] = []

    print("=" * 72)
    print("Gold layer orchestrator — starting")
    print("=" * 72)

    try:
        log_silver_pass_summary(spark)
        print()

        for job in GOLD_JOBS:
            result = run_gold_job(spark, job)
            results.append(result)
            if not result.success:
                logger.warning("Continuing Gold pipeline after %s failure", job.table_name)
                print(f"[gold] Continuing after failure on {job.table_name}")

        total_elapsed = time.perf_counter() - pipeline_start
        print_execution_summary(results, total_elapsed)

        return {result.table_name: result for result in results}

    except Exception as exc:
        logger.exception("Gold orchestrator failed: %s", exc)
        print(f"[gold] ERROR orchestrator failed: {exc}")
        raise


def main() -> int:
    """
    Run the Gold layer orchestrator.

    Returns:
        0 if all tables succeeded, 1 if any failed.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        results = create_gold_tables()
        return 0 if all(r.success for r in results.values()) else 1
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
