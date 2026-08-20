# Requirements Analysis

E-commerce sales data pipeline — Databricks Medallion Architecture (Bronze → Silver → Gold → Dashboard).

---

## 1. Problem Statement

Our e-commerce company receives **daily batch extracts** from three operational systems: a customer database (~10,000 rows), an order system (~100,000 rows), and a product catalog (~500 rows). Today, this data lives in separate CSV files dropped onto cloud storage (S3 or DBFS). Analysts cannot reliably answer basic business questions—*Which products drive the most revenue? Who are our highest-value customers? How should we segment customers for marketing?*—because the data is raw, inconsistent, and not validated before use.

I need a **Databricks pipeline** that lands this data without loss, applies consistent quality rules, and produces trusted aggregation tables and dashboard-ready SQL. The solution must run on **Databricks Community Edition**, process **100,000+ order rows** in batch, and **never silently discard bad records**—every row must be retained with a clear quality flag so we can audit failures and fix upstream systems.

---

## 2. Functional Requirements

### 2.1 Data Sources

| Source | File | Approx. volume | Description |
|--------|------|----------------|-------------|
| Customer database | `customers.csv` | ~10,000 rows | Customer master: ID, name, email, country, created date |
| Order system | `orders.csv` | ~100,000 rows | Transactions: order ID, customer ID, product ID, quantity, date, status, amount |
| Product catalog | `products.csv` | ~500 rows | Product master: ID, name, category, price, SKU |

Files arrive daily as CSV on **S3 or DBFS** (e.g. `dbfs:/mnt/sales/customers/`, or `/FileStore/data/` on Community Edition).

### 2.2 Bronze Layer — Raw Ingestion

**Purpose:** Land source data exactly as received, with ingestion audit metadata only.

| ID | Requirement |
|----|-------------|
| BR-01 | Ingest `customers.csv`, `orders.csv`, and `products.csv` from configured S3/DBFS paths |
| BR-02 | **No business transformations** — no type casting, deduplication, joins, or column renames |
| BR-03 | Write each source to a Delta Lake table: `bronze_customers`, `bronze_orders`, `bronze_products` |
| BR-04 | Append ingestion metadata: `ingested_at` (timestamp), `source_file` (path) |
| BR-05 | Log row count after each read and each write |
| BR-06 | Ingestion order: **customers and products before orders** (orders reference both via foreign keys) |
| BR-07 | Orchestration via `src/bronze/ingest_all.py` and individual scripts (`01_`–`03_`) |

**Out of scope for Bronze:** Data quality rules, aggregations, schema enforcement beyond what CSV parsing requires.

### 2.3 Silver Layer — Cleansing and Quality Checks

**Purpose:** Standardize types, apply four quality dimensions, and flag every row—**never delete failed rows**.

| ID | Requirement |
|----|-------------|
| SV-01 | Read from Bronze Delta tables; write to `silver_customers`, `silver_orders`, `silver_products` |
| SV-02 | Add or update column `quality_check_result` on every row (`PASS` or failure code) |
| SV-03 | **Completeness** (`01_quality_completeness.py`): required fields must be non-null (e.g. `customer_id`, `email` for customers; `order_id`, `customer_id`, `product_id`, `order_date` for orders) |
| SV-04 | **Uniqueness** (`02_quality_uniqueness.py`): primary keys unique per entity (`customer_id`, `order_id`, `product_id`); flag all rows involved in duplicates |
| SV-05 | **Referential integrity** (`04_quality_referential_integrity.py`): every `orders.customer_id` must exist in customers; every `orders.product_id` must exist in products |
| SV-06 | **Type validation** (`03_quality_type_validation.py`): enforce expected types and formats (numeric amounts, valid dates, email format where applicable) |
| SV-07 | Failed rows remain in Silver output with explicit failure codes (e.g. `FAIL_COMPLETENESS`, `FAIL_UNIQUENESS`, `FAIL_REFERENTIAL_INTEGRITY`, `FAIL_TYPE_VALIDATION`) |
| SV-08 | Log pass/fail counts per check and per table |
| SV-09 | Orchestration via `src/silver/create_silver_tables.py` |

**Policy (from `.cursorrules`):** Flag, don't delete. Downstream Gold reads only rows where `quality_check_result = 'PASS'`.

### 2.4 Gold Layer — Business Aggregations

**Purpose:** Produce three analytical Delta tables sourced **only from Silver**, using only quality-passed rows.

| ID | Requirement |
|----|-------------|
| GD-01 | **`gold_sales_by_product`** (`01_sales_by_product.sql`): total quantity sold and total revenue per product (join PASS orders to PASS products) |
| GD-02 | **`gold_revenue_by_customer`** (`02_revenue_by_customer.sql`): order count and total revenue per customer (join PASS orders to PASS customers) |
| GD-03 | **`gold_customer_segmentation`** (`04_customer_segmentation.sql`): segment customers by lifetime revenue (e.g. high / medium / low value tiers) |
| GD-04 | All Gold SQL uses **CTEs**, clear column aliases, and filters `quality_check_result = 'PASS'` |
| GD-05 | Orchestration via `src/gold/create_gold_tables.py` |
| GD-06 | Gold must not read Bronze tables directly |

### 2.5 Dashboard Layer — SQL Visualizations

**Purpose:** Provide at least three ready-to-run SQL queries for BI tools or Databricks SQL dashboards.

| ID | Requirement |
|----|-------------|
| DB-01 | **Top products by revenue** — bar chart source from `gold_sales_by_product` |
| DB-02 | **Top customers by revenue** — ranked table/chart from `gold_revenue_by_customer` |
| DB-03 | **Customer segment distribution** — pie or bar chart from `gold_customer_segmentation` (count by segment) |
| DB-04 | Additional query encouraged (e.g. revenue concentration, category breakdown) in `src/dashboard/dashboard_queries.sql` |
| DB-05 | Usage guide in `src/dashboard/DASHBOARD_GUIDE.md` |

---

## 3. Non-Functional Requirements

### 3.1 Performance

| ID | Requirement |
|----|-------------|
| NFR-P01 | Pipeline must process **100,000+ order rows** (plus 10K customers, 500 products) in a single daily batch without timeout on Databricks Community Edition |
| NFR-P02 | Use Spark/Delta for scalable reads and writes; avoid collect-to-driver patterns on full datasets |
| NFR-P03 | Log elapsed time per layer (Bronze ingest, Silver DQ, Gold build) for monitoring |
| NFR-P04 | Gold tables are **materialized Delta tables** (not repeated full scans at dashboard query time) |

### 3.2 Maintainability

| ID | Requirement |
|----|-------------|
| NFR-M01 | **Modular scripts** — one concern per file (e.g. one Bronze script per source, one Silver script per DQ dimension) |
| NFR-M02 | Numbered execution order (`01_`, `02_`, …) within each layer |
| NFR-M03 | Every Python function has type hints and docstrings; module-level docstrings explain purpose (per `.cursorrules`) |
| NFR-M04 | Configuration via constants/parameters (paths, table names)—no hardcoded secrets or environment-specific URLs |
| NFR-M05 | Design and data model documented in `design-notes.md` and `data-model.md`; changes to schema require doc updates |

### 3.3 Data Quality

| ID | Requirement |
|----|-------------|
| NFR-DQ01 | **Flag, don't delete** — all source rows appear in Silver with a `quality_check_result` value |
| NFR-DQ02 | Failure codes are human-readable and traceable to the check that failed |
| NFR-DQ03 | Pass/fail metrics logged after each quality script |
| NFR-DQ04 | Gold aggregations exclude non-PASS rows by explicit filter, not implicit drops in Silver |
| NFR-DQ05 | Row counts reconciled across layers: Bronze row count = Silver row count per entity; Gold counts ≤ PASS Silver order count |

### 3.4 Additional Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-01 | **Storage:** Delta Lake for all pipeline tables (ACID, schema evolution support) |
| NFR-02 | **Traceability:** Ingestion metadata on Bronze; quality flags on Silver |
| NFR-03 | **Operability:** Errors caught with try/except; failures logged before re-raise |
| NFR-04 | **Security:** No real customer PII in repo sample data; production credentials via secrets only |

---

## 4. Assumptions

| ID | Assumption |
|----|------------|
| A-01 | **CSV format:** UTF-8 encoded, header row present, comma-delimited; column names stable day-to-day |
| A-02 | **Batch only:** One daily drop per source; no real-time or streaming requirements |
| A-03 | **Platform:** Databricks **Community Edition** (or equivalent single-cluster batch environment) |
| A-04 | **Storage paths:** CSVs accessible from the cluster via DBFS mount or `/FileStore`; production may use S3 with equivalent paths |
| A-05 | **Volume:** Customer ~10K, orders ~100K, products ~500 rows per daily load (order of magnitude, not hard SLA) |
| A-06 | **Schema:** Columns match `data-model.md` (customer_id, order_id, product_id as keys; FKs on orders) |
| A-07 | **Full refresh or append:** Initial implementation uses overwrite or append per table with documented strategy; incremental CDC is out of scope |
| A-08 | **Deleted upstream records:** If a customer or product is removed from source CSVs but still referenced by historical orders, referential integrity behavior is defined in edge cases (see §5) |
| A-09 | **Currency and timezone:** Amounts in a single currency; dates in a consistent timezone (UTC unless specified) |
| A-10 | **No PII in development:** Sample and test data use synthetic names and emails only |

---

## 5. Edge Cases

### 5.1 CSV Has Extra or Missing Columns

| Scenario | Expected behavior |
|----------|-------------------|
| **Missing required column** (e.g. no `customer_id`) | Bronze: ingest available columns as-is (or fail fast with clear error if read schema is strict—document chosen behavior). Silver completeness check flags all rows `FAIL_COMPLETENESS` for missing required fields. |
| **Extra unknown column** | Bronze: land extra columns unchanged (preserve raw fidelity). Silver: ignore or pass through extras; do not fail solely for unknown columns unless they break type casting. |
| **Column reorder** | Bronze: read by header name, not position; row counts unchanged. |
| **Header rename** (e.g. `cust_id` vs `customer_id`) | Treated as missing column unless mapping layer added—**out of scope** unless explicit rename config is introduced in Silver. |

### 5.2 All Rows Fail Quality Checks

| Scenario | Expected behavior |
|----------|-------------------|
| Entire `silver_orders` table has no `PASS` rows | Gold tables that depend on orders produce **zero rows** (or empty aggregations), not errors. Pipeline run **completes successfully** with logged warning. |
| Dashboard queries | Return empty result sets; BI shows “no data” rather than failing |
| Operations | Pass/fail counts in logs alert operators; no silent success |

### 5.3 Foreign Keys Reference Deleted or Missing Parent Records

| Scenario | Expected behavior |
|----------|-------------------|
| Order references `customer_id` not in daily `customers.csv` | Silver referential integrity flags order `FAIL_REFERENTIAL_INTEGRITY`; row **retained** in `silver_orders` |
| Order references `product_id` not in `products.csv` | Same as above for product FK |
| Customer exists in Bronze but fails completeness/uniqueness | Order may pass FK to Bronze ID but Gold joins use **Silver PASS** parents only—orders referencing FAIL customers excluded from Gold revenue joins |
| Historical orders, current catalog omit discontinued product | Orders flag `FAIL_REFERENTIAL_INTEGRITY` unless product still present in Bronze products load; business may later add “orphan product” handling—document in `debugging-notes.md` if implemented |

### 5.4 Additional Edge Cases

| Scenario | Expected behavior |
|----------|-------------------|
| Duplicate `order_id` in same file | Uniqueness check flags **all** duplicate rows |
| Null or invalid numeric `total_amount` | Type validation flags `FAIL_TYPE_VALIDATION` |
| Empty CSV (header only) | Bronze writes zero rows; Silver/Gold complete with zero rows; logged |
| Duplicate daily run (same files re-ingested) | Documented idempotency strategy (overwrite Bronze vs append); row counts and DQ metrics must remain interpretable |

---

## 6. Acceptance Criteria by Layer

### 6.1 Bronze Layer

| # | Criterion | Verification |
|---|-----------|--------------|
| B-AC1 | All three CSV sources ingest from configured S3/DBFS paths | Run `ingest_all.py`; no unhandled exceptions |
| B-AC2 | Delta tables `bronze_customers`, `bronze_orders`, `bronze_products` exist | `SHOW TABLES` or `spark.table(...)` succeeds |
| B-AC3 | Row counts match source CSV row counts (±0) | Logged counts compared to `wc -l` minus header |
| B-AC4 | Columns match source; no business transforms applied | Schema diff vs raw CSV (except `ingested_at`, `source_file`) |
| B-AC5 | `ingested_at` and `source_file` populated on every row | SQL spot check |
| B-AC6 | Customers and products ingested before orders | Script order or orchestrator sequence |

### 6.2 Silver Layer

| # | Criterion | Verification |
|---|-----------|--------------|
| S-AC1 | Silver tables exist for all three entities | Table listing |
| S-AC2 | Every row has non-null `quality_check_result` | `COUNT(*) WHERE quality_check_result IS NULL` = 0 |
| S-AC3 | Completeness, uniqueness, referential integrity, and type validation scripts execute | All four scripts run without error |
| S-AC4 | Failed rows are flagged, not removed | Silver row count = Bronze row count per entity |
| S-AC5 | Referential failures detected for orphan `customer_id` / `product_id` | Test data with invalid FKs → expected failure codes |
| S-AC6 | Pass/fail counts logged per check | Log output review |
| S-AC7 | Valid synthetic data produces majority `PASS` on orders | `GROUP BY quality_check_result` on sample load |

### 6.3 Gold Layer

| # | Criterion | Verification |
|---|-----------|--------------|
| G-AC1 | Three Gold tables created: `gold_sales_by_product`, `gold_revenue_by_customer`, `gold_customer_segmentation` | Table listing |
| G-AC2 | Gold reads Silver only (never Bronze) | Code/SQL review |
| G-AC3 | Only `quality_check_result = 'PASS'` rows contribute to metrics | Compare Gold totals to manual sum on PASS Silver subset |
| G-AC4 | `gold_sales_by_product` has one row per product with sales | Row count vs distinct products with PASS orders |
| G-AC5 | `gold_revenue_by_customer` has revenue and order count per customer | Spot check against Silver |
| G-AC6 | `gold_customer_segmentation` assigns every qualifying customer to a segment | No NULL segment for customers with PASS orders |
| G-AC7 | SQL uses CTEs and clear aliases | Code review of `src/gold/*.sql` |

### 6.4 Dashboard Layer

| # | Criterion | Verification |
|---|-----------|--------------|
| D-AC1 | At least **three** visualization queries in `dashboard_queries.sql` | File review |
| D-AC2 | Queries run without error against Gold tables | Execute in Databricks SQL |
| D-AC3 | Top products and top customers queries return sensible rankings | Manual review on sample data |
| D-AC4 | Segmentation query returns counts per segment | Sum of segment counts ≤ customer count with orders |
| D-AC5 | `DASHBOARD_GUIDE.md` documents chart type and source table per query | Doc review |

### 6.5 End-to-End

| # | Criterion | Verification |
|---|-----------|--------------|
| E-AC1 | Full pipeline runs Bronze → Silver → Gold → Dashboard queries on ~100K orders | Single end-to-end run within Community Edition limits |
| E-AC2 | Total pipeline completes in acceptable batch window | Logged layer timings |
| E-AC3 | No real PII or credentials in repository | Security review of `data/` and config |

---

## Out of Scope

- Real-time / streaming ingestion
- Incremental CDC and slowly changing dimensions
- Business-logic quality rules beyond the four core checks (may be added in `05_quality_business_logic.py` separately)
- Daily/weekly trend tables (not required for this phase)
- Production job scheduling, alerting, and CI/CD (documented as future work in `design-notes.md`)
- Automated pytest suite (validation scripts and manual checks acceptable for assessment)

---

## Related Documents

| Document | Purpose |
|----------|---------|
| `data-model.md` | Entity schemas and relationships |
| `data-quality-strategy.md` | DQ dimension details and metrics |
| `design-notes.md` | Architecture and layer decisions |
| `.cursorrules` | Implementation standards (naming, flag-don't-delete, logging) |
| `tool-specific/cursor-workflow/spec.md` | Technical spec and dependencies |
