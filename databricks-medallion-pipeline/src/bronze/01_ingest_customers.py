"""
Bronze layer: ingest customers CSV into Delta table bronze_customers.

Reads data/customers.csv with schema inference, appends ingestion metadata only
(no business transformations), and writes to Delta using overwrite mode for
idempotent daily re-runs.

Databricks Community Edition compatible.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Allow imports when run as a script from repo root or Databricks workspace
if __name__ == "__main__" and str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

from bronze_helpers import get_spark, ingest_csv_to_bronze, IngestResult

logger = logging.getLogger(__name__)

SOURCE_FILENAME = "customers.csv"
TABLE_NAME = "bronze_customers"
APP_NAME = "bronze_ingest_customers"


def ingest_customers(
    spark: SparkSession,
    data_dir: str | None = None,
) -> IngestResult:
    """
    Ingest customers.csv into the bronze_customers Delta table.

    Args:
        spark: Active SparkSession.
        data_dir: Optional directory containing customers.csv.

    Returns:
        IngestResult with row counts, timing, and success flag.
    """
    return ingest_csv_to_bronze(
        spark=spark,
        source_filename=SOURCE_FILENAME,
        table_name=TABLE_NAME,
        data_dir=data_dir,
    )


def main() -> int:
    """
    Run customers Bronze ingestion as a standalone job.

    Returns:
        0 on success, 1 on failure.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark = get_spark(APP_NAME)
    result = ingest_customers(spark)

    if not result.success:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
