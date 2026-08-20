# E-Commerce Sales Medallion Pipeline

A Databricks Medallion Architecture pipeline for daily e-commerce sales data. Raw customer, order, and product CSVs land in **Bronze** as Delta tables, pass through **Silver** data quality checks (flag, never delete), roll up into **Gold** business metrics, and feed a **SQL Dashboard** for product, customer, and revenue analytics. Built for Databricks Community Edition with PySpark, Delta Lake, and reproducible sample data (~110,500 rows with ~700 intentional defects for testing).

---

## Architecture

```
  ┌─────────────────┐     ┌──────────────────────────────────────┐
  │  Source CSVs    │     │           BRONZE (Raw Delta)          │
  │                 │     │  bronze_customers                     │
  │ customers.csv ──┼────►│  bronze_orders      + ingestion meta  │
  │ orders.csv    ──┼────►│  bronze_products                      │
  │ products.csv  ──┼────►│  (inferSchema, overwrite + metadata)  │
  └─────────────────┘     └──────────────────┬───────────────────┘
                                             │
                                             ▼
                          ┌──────────────────────────────────────┐
                          │      SILVER (Validated + Flagged)     │
                          │  silver_customers / orders / products │
                          │  quality_check_result (PASS | FAIL_*) │
                          │  silver_quality_report                │
                          └──────────────────┬───────────────────┘
                                             │  PASS rows only
                                             ▼
                          ┌──────────────────────────────────────┐
                          │         GOLD (Aggregated)             │
                          │  gold_sales_by_product                │
                          │  gold_revenue_by_customer             │
                          │  gold_customer_segmentation           │
                          └──────────────────┬───────────────────┘
                                             │
                                             ▼
                          ┌──────────────────────────────────────┐
                          │   DASHBOARD (Databricks SQL)          │
                          │  Bar · Histogram · Pie · Line charts  │
                          └──────────────────────────────────────┘
```

**Policy:** Silver flags bad rows; Gold consumes `quality_check_result = 'PASS'` only.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| [Databricks Community Edition](https://community.cloud.databricks.com/) account | Single-cluster workspace; start compute before running jobs |
| **Python 3.9+** | Local data generation and pytest |
| **pyspark** | Bronze/Silver/Gold transforms (`requirements.txt`) |
| **delta-spark** | Delta table reads/writes |
| **faker** | Sample CSV generation |
| **pandas** | Optional — ad-hoc CSV inspection locally (not required by pipeline scripts) |
| **pytest** | Automated data quality tests |

Install local dependencies:

```bash
cd databricks-medallion-pipeline
pip install -r requirements.txt
```

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/birbal1254/TTN-C1-assessment.git
cd TTN-C1-assessment/databricks-medallion-pipeline
```

### 2. Generate sample data

```bash
python src/data_generation/generate_sample_data.py
```

Creates `data/customers.csv` (10,000 rows), `data/products.csv` (500 rows), and `data/orders.csv` (100,000 rows) with seed `42`.

### 3. Upload CSVs to DBFS

In Databricks, upload files to a DBFS path (e.g. `/FileStore/data/`):

**Option A — UI:** **Data** → **Create table** → **Upload File** → select the three CSVs.

**Option B — CLI / notebook:**

```python
# From a Databricks notebook (adjust local paths)
dbutils.fs.cp("file:/Workspace/Repos/<user>/TTN-C1-assessment/databricks-medallion-pipeline/data/customers.csv",
              "dbfs:/FileStore/data/customers.csv")
dbutils.fs.cp("file:/Workspace/Repos/<user>/TTN-C1-assessment/databricks-medallion-pipeline/data/orders.csv",
              "dbfs:/FileStore/data/orders.csv")
dbutils.fs.cp("file:/Workspace/Repos/<user>/TTN-C1-assessment/databricks-medallion-pipeline/data/products.csv",
              "dbfs:/FileStore/data/products.csv")
```

Set the Bronze data path (notebook or cluster env):

```python
import os
os.environ["BRONZE_DATA_PATH"] = "/dbfs/FileStore/data"
```

### 4. Run Bronze ingestion

Attach a cluster, then in a notebook under `src/bronze/`:

```python
%run ./ingest_all
```

Or run individual scripts: `01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py`.

**Verify:**

```sql
SELECT 'bronze_customers' AS t, COUNT(*) FROM bronze_customers
UNION ALL SELECT 'bronze_orders', COUNT(*) FROM bronze_orders
UNION ALL SELECT 'bronze_products', COUNT(*) FROM bronze_products;
-- Expected: 10000, 100000, 500
```

### 5. Run Silver quality checks

```python
%run ../silver/create_silver_tables
```

**Verify:**

```sql
SELECT quality_check_result, COUNT(*) FROM silver_orders GROUP BY 1 ORDER BY 2 DESC;
SELECT * FROM silver_quality_report;
```

### 6. Run Gold aggregations

```python
%run ../gold/create_gold_tables
```

**Verify:**

```sql
SELECT COUNT(*) FROM gold_sales_by_product;
SELECT COUNT(*) FROM gold_revenue_by_customer;
SELECT * FROM gold_customer_segmentation ORDER BY customer_count DESC;
```

### 7. Create the dashboard

1. Open **SQL** → **Dashboards** → **Create dashboard** (name: `E-Commerce Sales Analytics`).
2. Add visualizations from `src/dashboard/dashboard_queries.sql`.
3. Follow step-by-step layout instructions in `src/dashboard/DASHBOARD_GUIDE.md`.

---

## Project Structure

```
databricks-medallion-pipeline/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── .cursorrules                       # Cursor AI coding conventions
├── data/                              # Generated CSVs (gitignored contents optional)
├── database/
│   └── schema.sql                     # ecommerce_medallion DDL
├── src/
│   ├── data_generation/
│   │   └── generate_sample_data.py    # Faker-based CSV generator (seed=42)
│   ├── bronze/
│   │   ├── bronze_helpers.py          # Spark session, paths, Delta writes
│   │   ├── 01_ingest_customers.py
│   │   ├── 02_ingest_orders.py
│   │   ├── 03_ingest_products.py
│   │   └── ingest_all.py              # Bronze orchestrator
│   ├── silver/
│   │   ├── 01_quality_completeness.py
│   │   ├── 02_quality_uniqueness.py
│   │   ├── 03_quality_type_validation.py
│   │   ├── 04_quality_referential_integrity.py
│   │   ├── 05_quality_business_logic.py   # Scaffold (not in orchestrator)
│   │   └── create_silver_tables.py    # Silver orchestrator + quality report
│   ├── gold/
│   │   ├── 01_sales_by_product.sql / .py
│   │   ├── 02_revenue_by_customer.sql / .py
│   │   ├── 03_daily_weekly_trends.sql     # SQL only (not in orchestrator)
│   │   ├── 04_customer_segmentation.sql / .py
│   │   └── create_gold_tables.py
│   └── dashboard/
│       ├── dashboard_queries.sql      # 4 dashboard SQL queries
│       └── DASHBOARD_GUIDE.md         # CE dashboard setup guide
├── tests/
│   ├── conftest.py
│   └── test_data_quality.py           # 6 pytest cases
├── data-quality-strategy.md           # Full DQ specification
├── design-notes.md                    # Architecture and data model
├── debugging-notes.md                 # Fill-in debugging journal
├── requirements-analysis.md
└── tool-workflow.md                   # Cursor AI workflow for assessment
```

---

## Data Quality Summary

Silver runs four checks in order. Rows are **never deleted**; failures set `quality_check_result`. Gold uses **PASS** rows only.

| Check | Script | What it validates | Expected result (sample data) |
|-------|--------|-------------------|-------------------------------|
| **Completeness** | `01_quality_completeness.py` | Required columns non-null / non-blank | 50 NULL emails (customers); 100 NULL `customer_id`, 200 NULL `product_id` (orders) |
| **Uniqueness** | `02_quality_uniqueness.py` | Primary keys unique per table | 10 duplicate `customer_id` rows; 20 duplicate `order_id` rows (all rows in duplicate groups flagged) |
| **Type validation** | `03_quality_type_validation.py` | Dates, numerics, email format | ~100% pass (sample data has null/FK issues, not type corruption) |
| **Referential integrity** | `04_quality_referential_integrity.py` | Order FKs exist in customer/product tables | 50 orphan `customer_id` (99901–99950); 30 orphan `product_id` (9901–9930) |

**Overall profile**

| Metric | Value |
|--------|-------|
| Total pipeline rows | ~110,500 |
| Intentional defect injections | ~700 (~0.7% at check level) |
| Expected overall PASS rate | ~99.3% (±0.5%) |
| Products catalog | Clean (500 rows, no injected defects) |

See `data-quality-strategy.md` for detection logic, flag values, and SQL/PySpark examples.

---

## Dashboard Preview

Dashboard SQL lives in `src/dashboard/dashboard_queries.sql`. Suggested layout (see `DASHBOARD_GUIDE.md`):

| # | Visualization | Chart type | Source | What it shows |
|---|---------------|------------|--------|---------------|
| 1 | **Top 10 Products by Revenue** | Horizontal bar | `gold_sales_by_product` | Best-selling products by `total_revenue`, with quantity and order counts |
| 2 | **Customer Revenue Distribution** | Histogram / bar | `gold_revenue_by_customer` | Customer counts in revenue buckets ($0–100 through $10,000+) |
| 3 | **Customer Segmentation Breakdown** | Pie / donut | `gold_customer_segmentation` | Share of customers by segment (High-Value, Repeat, One-Time, Inactive, Regular) |
| 4 | **Monthly Revenue Trend** *(bonus)* | Line chart | `silver_orders` (PASS only) | `monthly_revenue` and order volume over time |

Filters you can add in the dashboard UI: product `category`, customer `country`, date range, and segment type.

---

## Testing

### Local (CSV-level checks — tests 1–4)

```bash
cd databricks-medallion-pipeline
pip install -r requirements.txt
pytest tests/test_data_quality.py -v
```

Tests 1–4 load sample CSVs into Spark DataFrames and assert Silver flag functions catch known defects (50 NULL emails, 100 NULL customer IDs, 20 duplicate order rows, 50+30 orphan FKs).

### Full pipeline (tests 5–6 — Delta tables required)

Run on a Databricks cluster after Bronze → Silver → Gold:

```python
%pip install pytest
```

```bash
pytest /Workspace/Repos/<user>/TTN-C1-assessment/databricks-medallion-pipeline/tests/test_data_quality.py -v
```

Tests 5–6 validate overall PASS rate (~99.3%) and row-count reconciliation across layers. They are skipped locally if Delta tables are unavailable.

---

## Known Limitations

| Limitation | Impact |
|------------|--------|
| **Databricks Community Edition** | One active cluster; SQL warehouse must be running; no production SLA |
| **Local pytest without Delta** | Tests 5–6 skip or fail without a configured Spark + Delta environment |
| **`03_daily_weekly_trends.sql`** | Defined but not wired into `create_gold_tables.py` orchestrator |
| **`05_quality_business_logic.py`** | Scaffold only — not executed by Silver orchestrator |
| **Schema inference on Bronze** | `inferSchema=True` can mis-type columns; Silver type checks compensate |
| **Intentional bad data** | Sample CSVs include ~700 defects by design — not suitable as production data |
| **Path configuration** | Must set `BRONZE_DATA_PATH` on Databricks; local defaults to `./data/` |
| **Unity Catalog** | Scripts use unqualified table names unless you configure `ecommerce_medallion` database from `database/schema.sql` |

For troubleshooting, use `debugging-notes.md` and `tool-workflow.md`.

---

## Documentation

| Document | Purpose |
|----------|---------|
| `requirements-analysis.md` | Functional requirements and acceptance criteria |
| `design-notes.md` | Architecture, data model, layer designs |
| `data-quality-strategy.md` | DQ rules, metrics, and flagging strategy |
| `debugging-notes.md` | Issue log template for development |
| `src/dashboard/DASHBOARD_GUIDE.md` | Dashboard creation on CE |
| `tool-workflow.md` | Cursor AI workflow for the assessment |

---

## Author & Date

**Author:** birbal1254 — TTN C1 Assessment  
**Date:** August 2026  

Update `candidate-info.md` with your contact details and skills summary.
