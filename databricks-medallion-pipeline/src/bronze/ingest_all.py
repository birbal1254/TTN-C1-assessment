"""
Bronze layer orchestrator: run all CSV ingestions into Delta tables.

Executes customers → products → orders (parent entities before orders FK sources).
Continues remaining jobs if one fails and prints a summary table at the end.

Databricks Community Edition compatible.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import List

if __name__ == "__main__" and str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

from bronze_helpers import IngestResult, get_spark

# Import ingest functions from numbered modules (Databricks / local script execution)
import importlib.util


def _load_ingest_function(module_filename: str, function_name: str):
    """Load an ingest function from a numbered Bronze script in this directory."""
    module_path = Path(__file__).parent / module_filename
    spec = importlib.util.spec_from_file_location(module_filename, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, function_name)


ingest_customers = _load_ingest_function("01_ingest_customers.py", "ingest_customers")
ingest_products = _load_ingest_function("03_ingest_products.py", "ingest_products")
ingest_orders = _load_ingest_function("02_ingest_orders.py", "ingest_orders")

logger = logging.getLogger(__name__)

APP_NAME = "bronze_ingest_all"


def ingest_all(data_dir: str | None = None) -> List[IngestResult]:
    """
    Run all Bronze ingestion jobs in dependency-safe order.

    Order: customers and products before orders (Silver referential checks expect
    parent entities to exist in Bronze first).

    Args:
        data_dir: Optional directory containing source CSV files.

    Returns:
        List of IngestResult for each job (success or failure).
    """
    spark = get_spark(APP_NAME)
    jobs = [
        ("bronze_customers", ingest_customers),
        ("bronze_products", ingest_products),
        ("bronze_orders", ingest_orders),
    ]

    results: List[IngestResult] = []
    pipeline_start = time.perf_counter()

    print("=" * 72)
    print("Bronze ingestion pipeline — starting")
    print("=" * 72)

    for label, ingest_fn in jobs:
        try:
            result = ingest_fn(spark, data_dir=data_dir)
        except Exception as exc:
            logger.exception("Unexpected failure running %s ingest", label)
            result = IngestResult(
                table_name=label,
                source_path="",
                row_count=0,
                elapsed_seconds=0.0,
                success=False,
                error_message=str(exc),
            )
            print(f"[bronze] ERROR {label}: unexpected failure — {exc}")

        results.append(result)

        if not result.success:
            # Continue other ingestions so partial pipeline progress is still available
            logger.warning("Continuing Bronze pipeline after %s failure", label)
            print(f"[bronze] Continuing after failure on {label}")

    total_elapsed = time.perf_counter() - pipeline_start
    print_summary(results, total_elapsed)

    return results


def print_summary(results: List[IngestResult], total_elapsed: float) -> None:
    """
    Print a tabular summary of Bronze ingestion results.

    Args:
        results: Per-table ingest outcomes.
        total_elapsed: Total wall-clock seconds for the orchestrator run.
    """
    print()
    print("=" * 72)
    print("Bronze ingestion summary")
    print("=" * 72)
    print(f"{'Table':<22} {'Row Count':>12} {'Time (s)':>10} {'Status':>10}")
    print("-" * 72)

    for result in results:
        status = "OK" if result.success else "FAILED"
        print(
            f"{result.table_name:<22} {result.row_count:>12,} "
            f"{result.elapsed_seconds:>10.2f} {status:>10}"
        )
        if result.error_message:
            print(f"  error: {result.error_message}")

    successes = sum(1 for r in results if r.success)
    print("-" * 72)
    print(f"Jobs succeeded: {successes}/{len(results)}")
    print(f"Total pipeline time: {total_elapsed:.2f}s")
    print("=" * 72)


def main() -> int:
    """
    Run the full Bronze ingestion orchestrator.

    Returns:
        0 if all jobs succeeded, 1 if any job failed.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    results = ingest_all()
    return 0 if all(r.success for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
