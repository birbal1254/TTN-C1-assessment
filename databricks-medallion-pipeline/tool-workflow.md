# Tool Workflow — AI Capability Assessment

This document describes how I use **Cursor** as my primary AI tool to build an e-commerce sales pipeline on Databricks using the Medallion Architecture (Bronze → Silver → Gold → Dashboard). It is written against this repository (`databricks-medallion-pipeline/`) and reflects what I actually do—not a generic AI playbook.

---

## 1. Primary AI Tool: Cursor

**Tool:** Cursor Desktop (check **Help → About** for your installed version; features below are from the current Composer/agent workflow).

**Why Cursor for this project:** The repo mixes Python (PySpark ingestion and DQ), SQL (Gold aggregations), Markdown (design docs), and CSV sample data. Cursor handles multi-file edits across `src/bronze/`, `src/silver/`, and `src/gold/` in one session, which matches how Medallion pipelines are built layer by layer.

### Features I use

| Feature | How I use it on this project |
|---------|------------------------------|
| **Chat** | Clarify requirements, ask “should this DQ rule live in Silver or Bronze?”, review design trade-offs before coding |
| **Composer / Agent** | Scaffold repo structure, implement `01_ingest_customers.py`, generate all five Silver DQ scripts, update `data-model.md` when schemas change |
| **Tab completion** | Finish repetitive PySpark patterns (`df.write.format("delta")...`), SQL `GROUP BY` blocks in `src/gold/*.sql`, and docstrings that match `.cursorrules` |

**Typical session flow:**

1. Open Chat → narrow the task (“only referential integrity for orders”).
2. Switch to Composer → implement across files with file references.
3. Use Tab completion while hand-editing edge cases AI missed.

**What I do not rely on AI for:** Choosing business definitions (e.g. what counts as `high_value` in customer segmentation)—I decide that, then ask AI to implement it.

---

## 2. How I Provide Project Context

AI output quality depends on constraints. I layer context so I do not repeat the same rules in every prompt.

### `.cursorrules`

Located at `databricks-medallion-pipeline/.cursorrules`. This is the highest-priority context. It encodes:

- Medallion layer boundaries (no CSV → Gold shortcuts)
- Table naming: `bronze_*`, `silver_*`, `gold_*`
- Required script structure: module docstring, try/except, logging, row counts
- DQ policy: **never delete bad rows**—use `quality_check_result`
- SQL style: CTEs, clear aliases

**Effect:** When I ask Composer to “implement Silver completeness checks,” it already knows not to drop null `customer_id` rows and to flag them instead.

### Design spec and docs

I point the agent at specific files rather than pasting long specs into Chat:

| File | Purpose |
|------|---------|
| `tool-specific/cursor-workflow/spec.md` | Scope, inputs/outputs, acceptance criteria |
| `design-notes.md` | Layer responsibilities, ingestion metadata on Bronze |
| `data-model.md` | Entities, FKs (`orders.customer_id` → `customers`) |
| `data-quality-strategy.md` | Maps DQ dimensions to `src/silver/01–05_*.py` |
| `requirements-analysis.md` | Functional vs non-functional requirements |

**Example prompt:**

> Read `data-model.md` and `design-notes.md`. Implement `src/bronze/02_ingest_orders.py` to read `data/orders.csv`, add `ingested_at` and `source_file`, and write to `bronze_orders` Delta. Follow `.cursorrules`.

### File references (@-mentions)

In Cursor Chat/Composer I reference paths explicitly:

- `@src/bronze/01_ingest_customers.py` — “match this pattern for products”
- `@src/silver/01_quality_completeness.py` — “same flagging approach for uniqueness”
- `@.cursorrules` — when generated code drifts from conventions

### Repo structure as context

The numbered scripts (`01_`, `02_`, …) signal execution order. I tell the agent:

> Bronze ingest order: customers and products before orders (FK referential checks in Silver assume parent tables exist).

---

## 3. How I Use AI for Requirement Analysis

Before coding, I use Chat to break the problem down and surface edge cases. I feed AI the placeholder requirements, then refine with project-specific questions.

### Breaking down the problem

**Prompt:**

> I have three CSVs: customers, orders, products. Orders reference customer_id and product_id. List functional requirements for a Medallion pipeline that produces: sales by product, revenue by customer, daily/weekly trends, and customer segmentation. Group by Bronze, Silver, Gold.

**What I expect back:** A layer-by-layer task list—which became `requirements-analysis.md` and `tool-specific/cursor-workflow/task-breakdown.md`.

### Identifying edge cases

**Prompt:**

> For e-commerce orders in `data/orders.csv`, list edge cases that Silver data quality should handle: null FKs, duplicate order_id, negative quantity, order_date in the future, total_amount inconsistent with quantity × price, invalid status values. For each, say whether we flag or quarantine per our `.cursorrules` policy.

**Edge cases I explicitly track for this project:**

| Scenario | Silver script | Expected handling |
|----------|---------------|-------------------|
| Null `customer_id` on order | `01_quality_completeness.py` | `quality_check_result = 'FAIL_COMPLETENESS'` |
| Duplicate `order_id` | `02_quality_uniqueness.py` | Flag duplicates; do not silently dedupe |
| `product_id` not in products | `04_quality_referential_integrity.py` | Flag orphan order |
| `quantity <= 0` | `05_quality_business_logic.py` | Flag invalid business row |

**Honest note:** AI sometimes suggests *deleting* bad rows. I correct that against `.cursorrules` and `data-quality-strategy.md` before any implementation.

---

## 4. How I Use AI for Pipeline Design

Medallion design is about **what transforms where**. I use AI to propose options, then I lock decisions in `design-notes.md`.

### Bronze layer decisions

**Design question:** Should Bronze cast types or preserve raw strings?

**My decision for this project:** Bronze preserves source fidelity; Silver enforces types. Bronze only adds audit columns (`ingested_at`, `source_file`).

**Prompt:**

> For Bronze ingestion of `customers.csv`, should I use inferSchema or an explicit StructType? Target is Databricks Community Edition. Bronze should not cleanse—only land raw data plus ingestion metadata.

### Silver layer decisions

**Design question:** One DQ script vs orchestrated modules?

**My decision:** Five separate scripts (`01`–`05`) plus `create_silver_tables.py` orchestrator—modular, testable, maps to `data-quality-strategy.md`.

**Prompt:**

> Propose a Silver design where each DQ dimension (completeness, uniqueness, types, referential integrity, business logic) is a separate PySpark module. All rows stay in output with a `quality_check_result` column. Gold should filter `PASS` only. Document which `gold_*` tables need that filter.

### Gold layer decisions

**Design question:** Materialized Delta tables vs views?

**My decision:** Materialized `gold_*` tables via SQL files + `create_gold_tables.py`—easier for dashboard queries and Community Edition SQL warehouse usage.

**Prompt:**

> Design four gold tables: sales_by_product, revenue_by_customer, daily_weekly_trends, customer_segmentation. Source only from `silver_*` tables. Use CTEs. Define segmentation tiers: high_value ≥ 1000, medium_value ≥ 100, else low_value.

### Dashboard layer

Gold outputs feed `src/dashboard/dashboard_queries.sql` and `DASHBOARD_GUIDE.md`. AI helps map each gold table to chart types (bar, line, pie)—I validate that analysts can run queries without touching Silver.

---

## 5. How I Use AI for Code Generation

I generate code with **explicit constraints** tied to this repo—not “write a PySpark pipeline.”

### Python / PySpark (Bronze example)

**Prompt:**

> Implement `src/bronze/01_ingest_customers.py`:
> - Read `data/customers.csv` with header
> - Add `ingested_at` (current timestamp) and `source_file` (path string)
> - Write Delta table `bronze_customers`, mode overwrite
> - Module docstring, type hints, docstrings on every function
> - try/except with logging module
> - Log row count after read and after write
> - Databricks Community Edition compatible—no Unity Catalog assumptions unless parameterized
> - Follow `.cursorrules` naming

### Silver DQ (flag, don't delete)

**Prompt:**

> Implement `src/silver/04_quality_referential_integrity.py` for orders:
> - Join `bronze_orders` to `bronze_customers` and `bronze_products`
> - If `customer_id` or `product_id` missing from parent, set `quality_check_result` to `FAIL_REFERENTIAL_INTEGRITY`
> - Otherwise keep existing result or set `PASS`
> - Log count of failures
> - Do not filter out failed rows

### SQL (Gold example)

**Prompt:**

> Complete `src/gold/01_sales_by_product.sql`:
> - CTE `valid_orders` from `silver_orders` where `quality_check_result = 'PASS'`
> - Join `silver_products` (also PASS only)
> - Output: product_id, product_name, category, total_quantity, total_revenue
> - CREATE OR REPLACE TABLE `gold_sales_by_product`
> - Clear column aliases, no nested subqueries

### Constraints I repeat when AI drifts

- Table names: `bronze_orders` not `bronze.orders` (unless I explicitly configure catalog)
- No `DROP` of bad records in Silver
- Gold never reads Bronze
- Every function needs a docstring—Tab completion often omits this on small helpers

---

## 6. How I Validate AI-Generated Code

I treat all AI output as a first draft. Validation is manual and incremental.

### Running locally / on Databricks

| Step | What I do |
|------|-----------|
| 1 | Run `src/data_generation/generate_sample_data.py` to populate `data/*.csv` with realistic rows **including intentional bad rows** for DQ |
| 2 | Run Bronze scripts or `ingest_all.py`; confirm Delta tables exist |
| 3 | Run Silver DQ + `create_silver_tables.py`; check `quality_check_result` distribution |
| 4 | Run `create_gold_tables.py` or execute SQL in Databricks SQL editor |
| 5 | Run `src/dashboard/dashboard_queries.sql` |

On Community Edition, I use a notebook that `%run` or imports each module, or paste SQL into the SQL warehouse.

### Checking logic (not just “it ran”)

**Row count reconciliation:**

```
bronze_orders count
  = silver_orders count (all rows retained)
  ≤ gold aggregations source rows (gold uses PASS only)
```

**Spot checks I run:**

```sql
-- Should return only PASS
SELECT quality_check_result, COUNT(*) FROM silver_orders GROUP BY 1;

-- Orphan orders should be flagged, not missing
SELECT * FROM silver_orders WHERE quality_check_result = 'FAIL_REFERENTIAL_INTEGRITY';
```

### Reviewing output

- Compare `gold_sales_by_product.total_revenue` to manual sum on PASS orders in a spreadsheet
- Verify segmentation counts match `gold_customer_segmentation` tier logic
- Read generated code for silent `dropDuplicates()` or `filter()` that removes DQ failures—common AI mistake

**What I fixed manually in early scaffolding:** AI initially used `bronze.customers` schema notation and suggested quarantine-only tables without retaining failures in Silver. I aligned naming and DQ policy with `.cursorrules`.

---

## 7. How I Use AI for Testing

This project does not yet have a full pytest suite; I use AI to generate **test data scenarios** and **validation scripts**.

### Generating test cases

**Prompt:**

> Extend `generate_sample_data.py` to produce:
> - 5 customers, 10 products, 30 orders
> - 3 orders with invalid customer_id
> - 2 duplicate order_id rows
> - 2 orders with quantity 0
> - 1 order with future order_date
> Output CSVs to `data/`. Use fake names/emails only.

### Validation scripts

**Prompt:**

> Write a PySpark validation script `src/silver/validate_dq_results.py` that:
> - Asserts every silver table has `quality_check_result` column
> - Asserts flagged referential failures = 3 for orders (from our test data)
> - Asserts no silver row count < bronze row count
> - Prints pass/fail summary

### AI-assisted test prompts for Gold

**Prompt:**

> Given test CSVs with known totals, write SQL assertions:
> - Sum of `gold_sales_by_product.total_revenue` equals sum of PASS `silver_orders.total_amount`
> - `gold_customer_segmentation` has exactly one row per customer with orders

**Honest limit:** AI-generated tests often assert happy paths only. I add at least one test per DQ script (`01`–`05`) myself.

---

## 8. How I Use AI for Debugging

When something fails, I paste **errors and context**—not “it’s broken.”

### Sharing error messages

**Prompt:**

> Running `02_ingest_orders.py` on Databricks Community Edition:
>
> ```
> AnalysisException: [DELTA_CREATE_TABLE_SCHEME_MISMATCH] Cannot create table ('`bronze_orders`').
> The specified schema does not match the schema of the existing table.
> ```
>
> Current write uses `inferSchema=True`. Table was created from an earlier run with different column order. How do I fix without losing Medallion audit columns? See `@src/bronze/02_ingest_orders.py`.

### Stack traces

**Prompt:**

> ```
> Py4JJavaError: ... NumberFormatException: For input string: "N/A"
> ```
> at `03_quality_type_validation.py` line 45 when casting `total_amount` to double. Some bronze rows have "N/A". Per `.cursorrules` we should flag not drop. Show PySpark code to add `FAIL_TYPE_VALIDATION` for non-numeric amounts.

### Root cause analysis

**Prompt:**

> `gold_revenue_by_customer` shows 0 rows but `silver_orders` has 500 PASS rows. Here is `02_revenue_by_customer.sql` and row counts per table. Is the JOIN key wrong or the quality filter too aggressive?

**Debugging habit:** I update `debugging-notes.md` with the root cause so I do not re-debug the same Community Edition limitation twice.

---

## 9. How I Use AI for Data Quality Checks

DQ is the core of Silver for this project. I collaborate with AI on **rule design** and **implementation**, always against `data-quality-strategy.md`.

### Designing checks

**Prompt:**

> For `customers.csv` fields (customer_id, name, email, country, created_at), define completeness rules: which columns are required? What `quality_check_result` values should we use? Align with `01_quality_completeness.py`.

### Completeness

Required non-null: `customer_id`, `email` (for customers); `order_id`, `customer_id`, `product_id`, `order_date` (for orders).

### Uniqueness

**Prompt:**

> Implement duplicate detection for `order_id` in `02_quality_uniqueness.py`. If duplicate, flag **all** duplicate rows (not just the second occurrence) so we can audit upstream issues.

### Referential integrity

Orders must reference existing `customer_id` and `product_id` in Bronze parent tables (loaded before orders).

**Prompt:**

> Orders ingested after customers and products. In `04_quality_referential_integrity.py`, use left joins to parents; flag orders where parent key is null or non-matching.

### Business logic

**Prompt:**

> In `05_quality_business_logic.py`, flag orders where:
> - quantity <= 0
> - total_amount < 0
> - order_date > current_date()
> - status not in ('completed', 'pending', 'cancelled')
> Combine multiple failures into one result string, e.g. `FAIL_BUSINESS_LOGIC:QUANTITY,STATUS`

### Gold impact

**Prompt:**

> List every `src/gold/*.sql` file and confirm each CTE filters `quality_check_result = 'PASS'`. If any miss it, patch them.

---

## 10. What I Avoid Sharing with AI

| Do not share | Why | What I use instead |
|--------------|-----|-------------------|
| Real customer PII | Privacy / compliance | Fake data from `generate_sample_data.py` (`Jane Doe`, `test@example.com`) |
| Production credentials | Security | Placeholders: `dbutils.secrets.get(scope, key)` without real values |
| Databricks workspace URLs with tokens | Accidental credential leak | “Community Edition cluster” without URL |
| Proprietary production schemas | IP / confidentiality | This project’s `data-model.md` and sample CSV headers only |
| Real revenue or order volumes | Business sensitivity | Small synthetic datasets (30–500 rows) |

**Rule:** If data came from prod, it does not go into Chat—even “just one row.”

---

## 11. Reusability: Production Pipelines

This assessment repo is smaller than production, but the **workflow scales**:

| Assessment practice | Production equivalent |
|---------------------|----------------------|
| `.cursorrules` | Team-wide rules + repo `AGENTS.md` / standards doc |
| `spec.md` + `data-model.md` | PRD, data contracts, OpenAPI/event schemas |
| Layer-scoped Composer tasks | Epic per layer (ingest, conform, aggregate) |
| `quality_check_result` column | Great Expectations, DLT expectations, or audit tables |
| Row count logging | Data observability (Monte Carlo, Databricks lineage, custom metrics) |
| Numbered `01_`, `02_` scripts | Databricks Jobs / Airflow DAG task order |
| AI prompt library (`ai-prompts/`) | Team prompt templates for onboarding |

**What changes in production:** Orchestration (Jobs not manual runs), secrets management, CI running validation scripts, code review on all AI-generated PySpark. **What stays the same:** Layer boundaries, explicit context files, never trusting first-pass AI code without row-count and DQ validation.

---

## 12. Lessons Learned

### What worked well

1. **`.cursorrules` before bulk codegen** — Reduced back-and-forth on naming (`bronze_*`), DQ policy (flag vs delete), and required logging/counts.
2. **Scaffolding the repo with Composer first** — Entire `databricks-medallion-pipeline/` tree and placeholders made later layer work incremental instead of one giant prompt.
3. **Mapping DQ to numbered Silver files** — AI could implement one dimension per chat without breaking others.
4. **File references over pasted schemas** — `@data-model.md` stayed in sync; pasted JSON schemas in Chat went stale.
5. **CTEs in Gold SQL** — AI-generated SQL was readable and matched dashboard needs.

### What needed manual correction

1. **DQ “helpfulness”** — AI often proposed `df.filter(...)` to remove bad rows. I rewrote to `withColumn("quality_check_result", when(...))`.
2. **Schema/table naming drift** — Mixed `bronze.orders`, `bronze_orders`, and catalog-qualified names. Standardized in `.cursorrules`.
3. **Over-engineering orchestration** — First suggestions used complex class hierarchies; I kept flat functions per script for Community Edition notebooks.
4. **Gold without PASS filter** — Early SQL joined all Silver rows and inflated revenue. Added explicit CTE filters.
5. **Docstrings and type hints** — Tab completion skipped them on helper functions; I added in review.
6. **Test data too clean** — Initial `generate_sample_data.py` produced only valid rows; DQ scripts looked correct but were never exercised until I asked AI for **intentional bad rows**.

### What I would do differently next time

- Add `validate_dq_results.py` in the same PR as each Silver script—not at the end.
- Keep a running `debugging-notes.md` entry per AI mistake pattern (schema mismatch, silent dedupe).
- Pin Databricks runtime version in `spec.md` so AI does not suggest APIs from newer runtimes.

---

## Quick Reference: Prompt Templates for This Project

```
# Context-heavy implementation
Read @.cursorrules, @data-model.md, and @design-notes.md. Implement [FILE]. [Specific constraints].

# DQ rule
In @src/silver/0N_*.py, flag rows where [condition]. Set quality_check_result to [VALUE]. Do not delete rows. Log failure counts.

# Gold SQL
Complete @src/gold/0N_*.sql using CTEs, silver_* sources only, filter quality_check_result = 'PASS', table name gold_*.

# Debug
Error: [paste]. File: @[path]. Expected: [behavior]. Row counts: bronze=X silver=Y gold=Z. Root cause and minimal fix.

# Test data
Update @generate_sample_data.py to include [N] bad rows for [DQ dimension]. Fake PII only.
```

---

## Related Files

| File | Role in workflow |
|------|------------------|
| `.cursorrules` | Persistent AI constraints |
| `tool-specific/cursor-workflow/spec.md` | Acceptance criteria |
| `ai-prompts/*.md` | Layer-specific prompt history |
| `debugging-notes.md` | Post-mortems |
| `reflection.md` | Broader retrospective |
| `final-ai-usage-summary.md` | Assessment summary export |
