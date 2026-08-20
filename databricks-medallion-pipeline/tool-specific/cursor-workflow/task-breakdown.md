# Task Breakdown — Cursor Workflow

How this e-commerce Medallion pipeline was decomposed into Cursor-friendly tasks.  
Each task is scoped for **one Composer/Agent session** or a focused Chat → implement loop.

**Total estimated effort:** ~23 hours across 7 phases  
**Repo:** `databricks-medallion-pipeline/`  
**Related:** `project-context.md`, `spec.md`, `tool-workflow.md`

---

## How to use this document

| Column | Meaning |
|--------|---------|
| **Prompt strategy** | What to attach (`@` files) and how to constrain the agent |
| **Expected output** | Files, tables, or artifacts that define "done" |
| **Validation method** | Commands, queries, or checks before moving to the next task |

**Rule:** One layer boundary per session. Re-attach `@.cursorrules` when starting a new phase.

---

## Phase 1: Setup (2 hours)

Establish repo structure, AI constraints, and requirements before any pipeline code.

---

### Task 1.1 — Initialize repo structure

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Agent prompt: *"Scaffold a Databricks Medallion Architecture repo for e-commerce CSV → Bronze → Silver → Gold → Dashboard. Include `src/bronze/`, `src/silver/`, `src/gold/`, `src/dashboard/`, `src/data_generation/`, `tests/`, `database/`, `data/`, `ai-prompts/`, and placeholder docs. Number scripts within each layer. No implementation yet — folders and README stub only."* Do **not** attach `.cursorrules` yet (created in 1.2). |
| **Expected output** | Directory tree matching project layout; stub `README.md`; empty numbered script placeholders; `requirements.txt` with pyspark, delta-spark, faker, pytest |
| **Validation method** | `find src -type f \| sort` — all layer folders exist; Bronze/Silver/Gold use `01_`, `02_` prefixes; no code reads CSV → Gold directly |

**Cursor tip:** Use **Agent** for multi-folder scaffold; avoid generating all layer code in this task.

---

### Task 1.2 — Create `.cursorrules`

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Chat → refine, then single-file write: *"Write `.cursorrules` for this Medallion pipeline: Python 3.9+, PySpark, SQL, Databricks Community Edition, table naming `bronze_*`/`silver_*`/`gold_*`, required docstrings/type hints/logging/row counts, **flag-don't-delete** DQ policy, Gold reads Silver only, SQL uses CTEs."* Attach `@design-notes.md` draft if available. |
| **Expected output** | `databricks-medallion-pipeline/.cursorrules` with architecture, naming, script structure, DQ policy, SQL guidelines |
| **Validation method** | Read file — confirm § flag-don't-delete, no CSV→Gold shortcut, CE compatibility stated; paste into new Chat and verify agent respects "never filter null customer_id" on a test prompt |

**Cursor tip:** This file is **Tier 1 context** for every subsequent task — do not skip.

---

### Task 1.3 — Write `requirements-analysis.md`

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Chat: *"Three CSVs: customers (~10K), orders (~100K), products (~500). Daily batch on DBFS/S3. Write functional requirements grouped by Bronze, Silver, Gold, Dashboard. Include problem statement, NFRs, assumptions, edge cases, acceptance criteria. Policy: flag bad rows, never delete."* Attach `@.cursorrules` + `@spec.md`. |
| **Expected output** | `requirements-analysis.md` with FR IDs (BR-*, SV-*, GD-*, DB-*), NFRs (CE, 100K+ rows), acceptance criteria checklist |
| **Validation method** | Cross-check against `spec.md` acceptance criteria; every Silver check maps to a future `01_`–`04_` script; Gold lists three required tables |

---

## Phase 2: Data Generation (3 hours)

Produce reproducible sample CSVs with intentional defects for DQ testing.

---

### Task 2.1 — Generate `customers.csv` with quality issues

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Composer: *"Implement customer generation in `src/data_generation/generate_sample_data.py`: 10,000 rows, Faker, seed=42. Inject **50 NULL emails** and **10 rows in duplicate `customer_id` groups**. Columns: customer_id, customer_name, email, country, signup_date, segment, lifetime_value. Output `data/customers.csv`."* Attach `@data-quality-strategy.md` defect table + `@.cursorrules`. |
| **Expected output** | Customer generator function; `data/customers.csv` with 10,000 data rows (+ header) |
| **Validation method** | `python -c` or SQL after load: `COUNT(*) = 10000`; `SUM(CASE WHEN email IS NULL THEN 1 END) = 50`; duplicate `customer_id` groups sum to 10 flagged rows |

---

### Task 2.2 — Generate `orders.csv` with quality issues

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Continue in same file: *"Add orders: 100,000 rows. Inject **100 NULL customer_id**, **200 NULL product_id**, **50 orphan customer_id** (99901–99950), **30 orphan product_id** (9901–9930), **10 duplicate order_id** pairs (20 rows in duplicate groups). Valid FKs reference generated customers/products."* Attach `@data-model.md`. |
| **Expected output** | Orders generator; `data/orders.csv` with 100,000 rows |
| **Validation method** | Row count = 100,000; NULL customer_id = 100; NULL product_id = 200; orphan customer_id in range 99901–99950 count = 50; duplicate order_id row count = 20 |

---

### Task 2.3 — Generate `products.csv`

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Add products: 500 rows, clean catalog (no intentional defects). Columns: product_id, product_name, category, price, cost, stock_quantity, reorder_level. Categories from fixed list. Output `data/products.csv`."* |
| **Expected output** | Products generator; `data/products.csv` with 500 rows |
| **Validation method** | Row count = 500; no NULL `product_id`; all product_ids referenced by valid orders exist |

---

### Task 2.4 — Validate generated data

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Chat: *"Write validation queries/script for sample data defect counts matching `data-quality-strategy.md`. Include total rows 110,500 and ~700 check-level issues."* Optionally extend `DATA_GENERATION_NOTES.md`. |
| **Expected output** | `src/data_generation/DATA_GENERATION_NOTES.md` with defect inventory; optional validation snippet in generator `if __name__ == "__main__"` block |
| **Validation method** | Run `python src/data_generation/generate_sample_data.py`; re-run with seed=42 — identical file hashes/row counts; manual spot-check orphan IDs |

---

## Phase 3: Bronze Layer (3 hours)

Land raw CSVs into Delta with ingestion metadata only.

---

### Task 3.1 — Ingest customers

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Composer: *"Implement `src/bronze/01_ingest_customers.py` + shared `bronze_helpers.py`: read CSV, `inferSchema=True`, add `ingestion_timestamp` + `source_file`, Delta overwrite + `overwriteSchema`, row count logging, CE-compatible Spark session."* Attach `@.cursorrules` + `@requirements-analysis.md` BR-* + `@design-notes.md`. |
| **Expected output** | `bronze_helpers.py`, `01_ingest_customers.py`; Delta table `bronze_customers` |
| **Validation method** | Local or CE: run script; `SELECT COUNT(*) FROM bronze_customers` = 10,000; `DESCRIBE bronze_customers` shows metadata columns; no rows dropped vs CSV |

---

### Task 3.2 — Ingest orders

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Implement `02_ingest_orders.py` — same pattern as `@src/bronze/01_ingest_customers.py`. Table `bronze_orders`. Support `BRONZE_DATA_PATH` env var for DBFS."* |
| **Expected output** | `02_ingest_orders.py`; `bronze_orders` with 100,000 rows |
| **Validation method** | Row count = 100,000; metadata columns populated; path resolves for `/dbfs/FileStore/data/orders.csv` on CE |

---

### Task 3.3 — Ingest products

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Implement `03_ingest_products.py` matching customers pattern → `bronze_products`."* Use Tab completion from 3.1 template. |
| **Expected output** | `03_ingest_products.py`; `bronze_products` with 500 rows |
| **Validation method** | Row count = 500; schema logged after read |

---

### Task 3.4 — Test ingestion

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Composer: *"Implement `src/bronze/ingest_all.py`: run customers → products → orders; continue on failure; print summary table. Idempotent re-run safe."* |
| **Expected output** | `ingest_all.py` orchestrator; combined row count 110,500 |
| **Validation method** | Run twice — no schema mismatch error; `UNION ALL` count query across three bronze tables; re-run after CSV regen with same seed — counts unchanged |

---

## Phase 4: Silver Layer (5 hours)

Apply four DQ dimensions; retain all rows with flags.

---

### Task 4.1 — Completeness check

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Composer: *"Implement `01_quality_completeness.py`: flag NULL/blank required fields per `data-quality-strategy.md`; set `quality_completeness` column; never drop rows."* Attach `@data-quality-strategy.md` Check 1 + `@.cursorrules` §8. |
| **Expected output** | Functions `flag_customers_completeness`, `flag_orders_completeness`, `flag_products_completeness` |
| **Validation method** | PySpark: 50 `FAIL: NULL email`; 100 `FAIL: NULL customer_id`; 200 `FAIL: NULL product_id`; row counts = Bronze counts |

---

### Task 4.2 — Uniqueness check

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Implement `02_quality_uniqueness.py`: flag **all rows** in duplicate PK groups (not just second occurrence). Keys: customer_id, order_id, product_id."* Reference `@tests/test_data_quality.py` expected 20 duplicate order rows. |
| **Expected output** | `02_quality_uniqueness.py` with `flag_uniqueness(df, key_col)` |
| **Validation method** | 10 customer duplicate rows flagged; 20 order duplicate rows flagged; 0 product duplicates |

---

### Task 4.3 — Type validation

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Implement `03_quality_type_validation.py`: dates, numerics ≥ 0, email regex, quantity ≥ 1 for orders. Flag `FAIL_TYPE_VALIDATION` — do not cast-and-drop."* |
| **Expected output** | `03_quality_type_validation.py` per-table validators |
| **Validation method** | Sample data ~100% pass on types (defects are null/FK, not corruption); no row count change; spot-check invalid email regex path with manual test row |

---

### Task 4.4 — Referential integrity

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Implement `04_quality_referential_integrity.py`: orders.customer_id → silver_customers, orders.product_id → silver_products. Only evaluate non-null FKs. Flag orphan IDs."* Attach `@data-model.md` FK diagram. |
| **Expected output** | `flag_orders_referential_integrity(orders, customers, products)` |
| **Validation method** | 50 orphan customer_id flags; 30 orphan product_id flags; pytest `test_referential_integrity_catches_orphans` passes |

---

### Task 4.5 — Combine and create Silver tables

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Agent: *"Implement `create_silver_tables.py`: run checks 01–04 in order; merge into `quality_check_result`; write `silver_customers`, `silver_orders`, `silver_products` Delta tables. Row count must equal Bronze."* Attach all four check modules. |
| **Expected output** | `create_silver_tables.py`; three Silver Delta tables with `quality_check_result` |
| **Validation method** | `silver_*` counts = `bronze_*` counts; `GROUP BY quality_check_result` shows ~99.3% PASS (±0.5%); no `filter()` removing failures in code review |

---

### Task 4.6 — Generate quality report

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Add `silver_quality_report` table or DataFrame: per-table, per-check pass/fail counts and pass rates. Log summary at end of orchestrator."* Align metrics with `@data-quality-strategy.md`. |
| **Expected output** | `silver_quality_report` table; console/log summary |
| **Validation method** | Report row counts match manual `GROUP BY` on flag columns; completeness/uniqueness/referential numbers match expected defect table |

---

## Phase 5: Gold Layer (4 hours)

Business aggregations from Silver PASS rows only.

---

### Task 5.1 — Sales by product aggregation

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Composer: *"Complete `src/gold/01_sales_by_product.sql` + `.py`: CTE `valid_orders` with `quality_check_result = 'PASS'`; join PASS products; output product_id, product_name, category, total_quantity, total_revenue, total_orders."* Attach `@.cursorrules` SQL section + `@design-notes.md`. |
| **Expected output** | `01_sales_by_product.sql`, `01_sales_by_product.py`; table `gold_sales_by_product` |
| **Validation method** | `SUM(total_revenue)` ≤ `SUM(total_amount) FROM silver_orders WHERE quality_check_result = 'PASS'`; no Bronze table references in SQL |

---

### Task 5.2 — Revenue by customer aggregation

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Implement `02_revenue_by_customer.sql` + `.py`: per customer — total_orders, total_revenue, avg_order_value. PASS orders + PASS customers only. Handle zero-order customers explicitly."* |
| **Expected output** | `02_revenue_by_customer.sql`, `.py`; `gold_revenue_by_customer` |
| **Validation method** | Customer count ≤ 10,000; `avg_order_value` NULL check query returns 0 rows after fix; revenue reconciles to PASS silver sum |

---

### Task 5.3 — Customer segmentation

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Implement `04_customer_segmentation.sql` + `.py`: segment summary table (High-Value, Repeat, One-Time, Inactive, Regular) with customer_count, total_revenue, pct_of_total. Source PASS silver orders + customers."* |
| **Expected output** | `04_customer_segmentation.sql`, `.py`; `gold_customer_segmentation` |
| **Validation method** | Segments sum to total customers with orders; tier logic matches `design-notes.md`; pie-chart query runs without error |

---

### Task 5.4 — Validate aggregations

| Field | Detail |
|-------|--------|
| **Prompt strategy** | *"Implement `create_gold_tables.py`: orchestrate 5.1–5.3; per-table validation (row counts, sample rows, timing); graceful failure per table; log PASS-only source counts."* |
| **Expected output** | `create_gold_tables.py`; all three Gold tables populated |
| **Validation method** | Run orchestrator on CE; reconciliation SQL from README; manual compare top-5 products to ad-hoc Silver aggregation |

---

## Phase 6: Dashboard (2 hours)

SQL queries and CE dashboard setup guide.

---

### Task 6.1 — Write dashboard queries

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Chat → Composer: *"Write `src/dashboard/dashboard_queries.sql` with 4 queries: (1) Top 10 products bar chart, (2) customer revenue histogram, (3) segmentation pie, (4) monthly revenue line from PASS silver_orders. Include chart-type comments."* Attach `@gold_*` table schemas. |
| **Expected output** | `dashboard_queries.sql` with Query 1–4, axis/filter comments |
| **Validation method** | Run each query in Databricks SQL editor — no errors; Query 1 returns 10 rows; Query 4 filters `quality_check_result = 'PASS'` |

---

### Task 6.2 — Create visualizations

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Manual on CE + Chat for chart config: *"Map each query in `@dashboard_queries.sql` to Databricks SQL dashboard visualization types (horizontal bar, histogram, pie, line)."* |
| **Expected output** | Live dashboard `E-Commerce Sales Analytics` with 3–4 tiles on CE |
| **Validation method** | Screenshot or checklist: each tile renders non-empty data; filters documented; cluster/SQL warehouse running |

---

### Task 6.3 — Document dashboard setup

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Agent: *"Write `src/dashboard/DASHBOARD_GUIDE.md`: CE prerequisites, navigate to SQL Dashboards, step-by-step tile creation, query mapping, troubleshooting (warehouse not running, empty tiles)."* |
| **Expected output** | `DASHBOARD_GUIDE.md` with step-by-step CE instructions |
| **Validation method** | Another user (or fresh CE session) can follow guide without repo context; links to `dashboard_queries.sql` line numbers |

---

## Phase 7: Testing & Documentation (4 hours)

Automated tests, README, reflection, and prompt archive.

---

### Task 7.1 — Write data quality tests

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Composer: *"Write `tests/test_data_quality.py` + `conftest.py`: 6 tests — completeness (50 emails, 100 customer_id), uniqueness (20 order dup rows), referential (50+30 orphans), overall PASS rate ~99.3%, row reconciliation. Tests 1–4 from CSV; 5–6 need Delta pipeline."* Attach `@data-quality-strategy.md` expected counts. |
| **Expected output** | `tests/test_data_quality.py`, `tests/conftest.py` |
| **Validation method** | `pytest tests/test_data_quality.py -v` — tests 1–4 pass locally; 5–6 pass on Databricks after full pipeline; no false positives on seed=42 data |

---

### Task 7.2 — Write README

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Agent: *"Comprehensive README: description, ASCII architecture, prerequisites, 7-step quick start, project tree, DQ summary table, dashboard preview, testing, limitations, author/date."* Attach `@README.md` stub + `@design-notes.md`. |
| **Expected output** | `README.md` (~300 lines) |
| **Validation method** | Quick start steps match actual script paths; row counts and defect numbers match `data-quality-strategy.md`; clone URL correct |

---

### Task 7.3 — Complete reflection

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Manual fill-in using `@reflection.md` framework: AI lifecycle (requirements → docs), top 3 AI wins, mistakes corrected, validation loop, reusable prompts. Chat can help draft bullets — **you** must verify accuracy. |
| **Expected output** | Completed `reflection.md`; optional updates to `final-ai-usage-summary.md` |
| **Validation method** | Every "AI got wrong" example references a real incident from `debugging-notes.md`; percentages/claims match git history honestly |

---

### Task 7.4 — Organize prompt history

| Field | Detail |
|-------|--------|
| **Prompt strategy** | Agent: *"Populate `ai-prompts/` (bronze, silver, gold, dashboard, debugging, documentation) with reusable prompt templates extracted from `tool-workflow.md`. Update `tool-specific/cursor-workflow/project-context.md` and this task-breakdown."* |
| **Expected output** | Filled `ai-prompts/*.md`; updated `project-context.md`, `task-breakdown.md`, `cursor-rules-or-instructions.md` |
| **Validation method** | Each ai-prompts file has ≥1 copy-paste prompt with `@` file references; prompts enforce `.cursorrules` constraints |

---

## Phase summary

| Phase | Hours | Tasks | Key deliverable |
|-------|-------|-------|-----------------|
| 1 Setup | 2 | 1.1–1.3 | `.cursorrules`, `requirements-analysis.md` |
| 2 Data | 3 | 2.1–2.4 | `data/*.csv` (~110,500 rows, ~700 defects) |
| 3 Bronze | 3 | 3.1–3.4 | `bronze_*` Delta tables, `ingest_all.py` |
| 4 Silver | 5 | 4.1–4.6 | `silver_*` + `silver_quality_report` |
| 5 Gold | 4 | 5.1–5.4 | `gold_*` tables, `create_gold_tables.py` |
| 6 Dashboard | 2 | 6.1–6.3 | `dashboard_queries.sql`, `DASHBOARD_GUIDE.md` |
| 7 Test & Docs | 4 | 7.1–7.4 | pytest suite, README, reflection, ai-prompts |
| **Total** | **23** | **28 tasks** | End-to-end CE pipeline + assessment artifacts |

---

## Dependency graph

```
Phase 1 (Setup)
    │
    ▼
Phase 2 (Data) ──► Phase 3 (Bronze)
                        │
                        ▼
                   Phase 4 (Silver)
                        │
                        ▼
                   Phase 5 (Gold)
                        │
                        ▼
                   Phase 6 (Dashboard)
                        │
                        ▼
                   Phase 7 (Testing & Docs)  ← can start 7.1 after Phase 4; 7.2–7.4 after Phase 6
```

---

## Cursor session checklist (per task)

```
[ ] Re-attach @.cursorrules
[ ] Attach one design doc (requirements / DQ strategy / design-notes)
[ ] Attach one reference implementation file from prior task
[ ] State single-task scope + "do not modify unrelated layers"
[ ] Run validation method before marking complete
[ ] Log failures in debugging-notes.md
```

---

## Out of scope (deferred tasks)

| Item | Notes |
|------|-------|
| `05_quality_business_logic.py` | Scaffold only — not in Phase 4 orchestrator |
| `03_daily_weekly_trends.sql` | SQL exists; not wired into `create_gold_tables.py` |
| Unity Catalog / `ecommerce_medallion` database | Optional; scripts use unqualified table names |
| Streaming / incremental loads | Batch-only for assessment |

---

**Last updated:** August 2026
