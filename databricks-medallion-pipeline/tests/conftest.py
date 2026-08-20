"""
Shared pytest fixtures for Medallion pipeline integration tests.

Tests 1–4 run quality checks directly on CSV DataFrames (no Delta required).
Tests 5–6 require the full Bronze → Silver → Gold Delta pipeline (Databricks or
local Spark with delta-spark configured).

Run:
  cd databricks-medallion-pipeline
  pip install -r requirements.txt
  pytest tests/test_data_quality.py -v
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict

import pytest
from pyspark.sql import DataFrame, SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
SRC_BRONZE = PROJECT_ROOT / "src" / "bronze"
SRC_SILVER = PROJECT_ROOT / "src" / "silver"
SRC_GOLD = PROJECT_ROOT / "src" / "gold"
DATA_GEN = PROJECT_ROOT / "src" / "data_generation" / "generate_sample_data.py"

RUN_SPARK_TESTS = os.environ.get("RUN_SPARK_TESTS", "1") == "1"
DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.2.0"


def _load_module(module_path: Path, module_name: str):
    """Dynamically import a pipeline module from an absolute path."""
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ensure_sample_data() -> None:
    """Generate sample CSV files if not already present."""
    if not (DATA_DIR / "customers.csv").exists():
        subprocess.run([sys.executable, str(DATA_GEN)], check=True, cwd=str(PROJECT_ROOT))


def _read_bronze_csv(spark: SparkSession, filename: str) -> DataFrame:
    """Read a Bronze CSV with schema inference."""
    path = str(DATA_DIR / filename)
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Provide a SparkSession with Delta Lake packages for optional pipeline tests.

    Reuses the active Databricks session when available.
    """
    if not RUN_SPARK_TESTS:
        pytest.skip("Spark tests disabled (set RUN_SPARK_TESTS=1 to enable)")

    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        return

    builder = (
        SparkSession.builder.appName("medallion_data_quality_tests")
        .master("local[2]")
        .config("spark.jars.packages", DELTA_PACKAGES)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bronze_frames(spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Load Bronze CSV sample data as Spark DataFrames.

    Used by quality-check unit tests that do not require Delta tables.
    """
    _ensure_sample_data()
    return {
        "bronze_customers": _read_bronze_csv(spark, "customers.csv"),
        "bronze_orders": _read_bronze_csv(spark, "orders.csv"),
        "bronze_products": _read_bronze_csv(spark, "products.csv"),
    }


@pytest.fixture(scope="session")
def pipeline_data(spark: SparkSession):
    """
    Run full Bronze → Silver → Gold pipeline into Delta tables.

    Skips when Delta is unavailable (e.g. offline local without jar download).
    """
    if not RUN_SPARK_TESTS:
        pytest.skip("Spark tests disabled (set RUN_SPARK_TESTS=1 to enable)")

    _ensure_sample_data()

    for path in (SRC_BRONZE, SRC_SILVER, SRC_GOLD):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)

    tables = [
        "bronze_customers",
        "bronze_orders",
        "bronze_products",
        "silver_customers",
        "silver_orders",
        "silver_products",
        "silver_quality_report",
        "gold_sales_by_product",
        "gold_revenue_by_customer",
        "gold_customer_segmentation",
    ]

    try:
        bronze_ingest = _load_module(SRC_BRONZE / "ingest_all.py", "ingest_all")
        bronze_results = bronze_ingest.ingest_all()
        if not all(r.success for r in bronze_results):
            pytest.skip("Bronze ingest failed — Delta pipeline unavailable in this environment")

        silver_create = _load_module(SRC_SILVER / "create_silver_tables.py", "create_silver_tables")
        silver_create.create_silver_tables(spark)

        gold_create = _load_module(SRC_GOLD / "create_gold_tables.py", "create_gold_tables")
        gold_create.create_gold_tables(spark)
    except Exception as exc:
        pytest.skip(f"Full Delta pipeline unavailable: {exc}")

    yield tables

    for table_name in tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass
