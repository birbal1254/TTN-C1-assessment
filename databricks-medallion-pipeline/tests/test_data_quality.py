"""
Integration tests verifying Silver data quality checks catch intentional sample defects.

Test cases 1–4 validate individual quality functions against sample CSV data.
Test cases 5–6 require the full Delta pipeline (Databricks or local with delta-spark).

Run locally:
  cd databricks-medallion-pipeline
  pip install -r requirements.txt
  pytest tests/test_data_quality.py -v

Run on Databricks (attach cluster, then in notebook terminal):
  %pip install pytest
  pytest /Workspace/Repos/.../databricks-medallion-pipeline/tests/test_data_quality.py -v
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_SILVER = PROJECT_ROOT / "src" / "silver"

TOTAL_PIPELINE_ROWS = 110_500
EXPECTED_ISSUE_INJECTIONS = 700
EXPECTED_PASS_RATE_PCT = 99.3
PASS_RATE_TOLERANCE_PCT = 0.5


def _load_silver_function(filename: str, function_name: str):
    """Import a callable from a numbered Silver quality module."""
    if str(SRC_SILVER) not in sys.path:
        sys.path.insert(0, str(SRC_SILVER))
    module_path = SRC_SILVER / filename
    spec = importlib.util.spec_from_file_location(filename, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, function_name)


class TestDataQuality:
    """Verify intentional sample data quality defects are detected by Silver checks."""

    def test_completeness_catches_null_emails(self, spark: SparkSession, bronze_frames) -> None:
        """Exactly 50 customer rows should fail completeness for NULL email."""
        flag_customers = _load_silver_function(
            "01_quality_completeness.py",
            "flag_customers_completeness",
        )
        flagged = flag_customers(bronze_frames["bronze_customers"])

        null_email_flags = flagged.filter(
            col("quality_completeness") == lit("FAIL: NULL email")
        ).count()

        assert null_email_flags == 50, (
            f"Expected 50 NULL email flags, found {null_email_flags}"
        )

    def test_completeness_catches_null_customer_id(
        self,
        spark: SparkSession,
        bronze_frames,
    ) -> None:
        """Exactly 100 order rows should fail completeness for NULL customer_id."""
        flag_orders = _load_silver_function(
            "01_quality_completeness.py",
            "flag_orders_completeness",
        )
        flagged = flag_orders(bronze_frames["bronze_orders"])

        null_customer_flags = flagged.filter(
            col("quality_completeness") == lit("FAIL: NULL customer_id")
        ).count()

        assert null_customer_flags == 100, (
            f"Expected 100 NULL customer_id flags, found {null_customer_flags}"
        )

    def test_uniqueness_catches_duplicate_orders(
        self,
        spark: SparkSession,
        bronze_frames,
    ) -> None:
        """Exactly 20 order rows should fail uniqueness (10 duplicate order_id pairs)."""
        flag_uniqueness = _load_silver_function(
            "02_quality_uniqueness.py",
            "flag_uniqueness",
        )
        flagged = flag_uniqueness(bronze_frames["bronze_orders"], "order_id")

        duplicate_flags = flagged.filter(
            col("quality_uniqueness") == lit("FAIL: DUPLICATE order_id")
        ).count()

        assert duplicate_flags == 20, (
            f"Expected 20 duplicate order_id flags, found {duplicate_flags}"
        )

    def test_referential_integrity_catches_orphans(
        self,
        spark: SparkSession,
        bronze_frames,
    ) -> None:
        """Referential check should flag 50 orphan customer_ids and 30 orphan product_ids."""
        flag_referential = _load_silver_function(
            "04_quality_referential_integrity.py",
            "flag_orders_referential_integrity",
        )
        flagged = flag_referential(
            bronze_frames["bronze_orders"],
            bronze_frames["bronze_customers"],
            bronze_frames["bronze_products"],
        )

        orphan_customer = flagged.filter(
            col("quality_referential_integrity") == lit("FAIL: ORPHAN customer_id")
        ).count()
        orphan_product = flagged.filter(
            col("quality_referential_integrity") == lit("FAIL: ORPHAN product_id")
        ).count()

        assert orphan_customer == 50, (
            f"Expected 50 orphan customer_id flags, found {orphan_customer}"
        )
        assert orphan_product == 30, (
            f"Expected 30 orphan product_id flags, found {orphan_product}"
        )

    def test_silver_pass_rate(self, spark: SparkSession, pipeline_data) -> None:
        """Overall Silver PASS rate should be ~99.3% (~700 issues / 110,500 rows)."""
        silver_tables = ["silver_customers", "silver_orders", "silver_products"]

        total_rows = 0
        pass_rows = 0
        for table_name in silver_tables:
            df = spark.table(table_name)
            total_rows += df.count()
            pass_rows += df.filter(col("quality_check_result") == lit("PASS")).count()

        assert total_rows == TOTAL_PIPELINE_ROWS, (
            f"Expected {TOTAL_PIPELINE_ROWS} Silver rows, found {total_rows}"
        )

        pass_rate_pct = (pass_rows / total_rows) * 100.0
        assert pass_rate_pct == pytest.approx(
            EXPECTED_PASS_RATE_PCT, abs=PASS_RATE_TOLERANCE_PCT
        ), (
            f"Expected pass rate ~{EXPECTED_PASS_RATE_PCT}%, found {pass_rate_pct:.2f}% "
            f"({pass_rows}/{total_rows} PASS; ~{EXPECTED_ISSUE_INJECTIONS} injected issues)"
        )

    def test_gold_uses_only_pass_rows(self, spark: SparkSession, pipeline_data) -> None:
        """Gold revenue totals must match aggregates from Silver PASS orders only."""
        silver_pass_revenue = (
            spark.table("silver_orders")
            .filter(col("quality_check_result") == lit("PASS"))
            .agg(spark_sum(col("total_amount").cast("double")).alias("revenue"))
            .collect()[0]["revenue"]
        )

        gold_product_revenue = (
            spark.table("gold_sales_by_product")
            .agg(spark_sum("total_revenue").alias("revenue"))
            .collect()[0]["revenue"]
        )

        gold_customer_revenue = (
            spark.table("gold_revenue_by_customer")
            .agg(spark_sum("total_revenue").alias("revenue"))
            .collect()[0]["revenue"]
        )

        assert gold_product_revenue is not None
        assert gold_customer_revenue is not None
        assert silver_pass_revenue is not None

        assert float(gold_product_revenue) == pytest.approx(float(silver_pass_revenue), rel=1e-4)
        assert float(gold_customer_revenue) == pytest.approx(float(silver_pass_revenue), rel=1e-4)

        segment_total = (
            spark.table("gold_customer_segmentation")
            .agg(spark_sum("customer_count").alias("cnt"))
            .collect()[0]["cnt"]
        )
        pass_customers_with_orders = spark.table("gold_revenue_by_customer").count()

        assert segment_total <= pass_customers_with_orders
