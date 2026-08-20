# Debugging Notes

Fill in this document as you build and run the e-commerce Medallion pipeline.  
**Project:** `databricks-medallion-pipeline/` | **Stack:** PySpark, Delta Lake, Databricks Community Edition

---

## 1. Issue Log

Track every significant bug or pipeline failure here. Add a new row as issues occur.

| # | Date | Issue Description | Root Cause | Resolution | AI Helped? |
|---|------|-------------------|------------|------------|------------|
| 1 | _YYYY-MM-DD_ | _Brief summary of what broke_ | _Why it happened_ | _What fixed it_ | _Yes / No — how_ |
| 2 | | | | | |
| 3 | | | | | |
| 4 | | | | | |
| 5 | | | | | |

### Example entries (replace or keep as reference)

| # | Date | Issue Description | Root Cause | Resolution | AI Helped? |
|---|------|-------------------|------------|------------|------------|
| E1 | 2026-01-15 | Silver job failed: `ConcurrentAppendException` on `silver_orders` | Two notebook runs overlapped — both tried to `MERGE`/`overwrite` the same Delta table | Serialized Silver runs in orchestrator; added `create_silver_tables.py` guard to skip if job already running; re-ran from Bronze after `DESCRIBE HISTORY` confirmed no partial commit | Yes — pasted stack trace; Cursor identified concurrent write and suggested single-writer pattern |
| E2 | 2026-01-16 | Bronze `customers.csv` ingest: garbled names (`JosÃ©`, `MÃ¼ller`) | CSV saved as UTF-8 with BOM or opened/re-saved in Excel as Latin-1; Spark read default encoding mismatched file | Re-exported CSV as UTF-8 without BOM; added `.option("encoding", "UTF-8")` on read; spot-checked `SELECT customer_name FROM bronze_customers WHERE customer_name LIKE '%Ã%'` | Yes — pasted sample rows + `@src/bronze/01_ingest_customers.py`; got encoding option and validation query |
| E3 | 2026-01-17 | `gold_revenue_by_customer` showed NULL `avg_order_value` for some rows | `AVG(total_amount)` over empty groups or `total_orders = 0`; division by zero edge case not filtered | Filtered to customers with ≥1 PASS order before aggregation; used `COALESCE` only where business rule allows; added assert in `create_gold_tables.py` | No — found via `SELECT * FROM gold_revenue_by_customer WHERE avg_order_value IS NULL` |

---

## 2. Common Issues Encountered

Check these first when something fails. Tick or annotate as you hit each one.

### Schema inference problems

| Symptom | Likely cause | Quick check |
|---------|--------------|-------------|
| Numeric column treated as STRING | Leading zeros, mixed formats, or `"N/A"` in CSV | `SELECT typeof(price), price FROM bronze_products LIMIT 20` |
| Date column is NULL after read | Wrong format (`DD/MM/YYYY` vs `YYYY-MM-DD`) | `SELECT signup_date, COUNT(*) FROM bronze_customers GROUP BY 1` |
| Unexpected extra columns | Source file changed; inference picks up new headers | Compare `DESCRIBE bronze_orders` to `data/orders.csv` header |
| Schema mismatch on overwrite | Delta table exists with different column set | `DESCRIBE TABLE EXTENDED bronze_orders` vs new DataFrame `.printSchema()` |

**My notes:**

```
_Add your schema-related issues here._
```

---

### NULL handling in joins

| Symptom | Likely cause | Quick check |
|---------|--------------|-------------|
| Join returns fewer rows than expected | INNER JOIN drops rows with NULL FK | `SELECT COUNT(*) FROM silver_orders WHERE customer_id IS NULL` |
| Referential check flags too many rows | NULL FK counted as orphan instead of completeness failure | Review check order: completeness before referential in `create_silver_tables.py` |
| Aggregation returns NULL | `SUM`/`AVG` over empty set or NULL inputs | `SELECT COUNT(*), SUM(total_amount) FROM silver_orders WHERE quality_check_result = 'PASS'` |
| Gold revenue doesn't match manual sum | Failed rows included or PASS filter missing | Compare PASS vs ALL: `GROUP BY quality_check_result` |

**My notes:**

```
_Add your NULL/join issues here._
```

---

### Delta table overwrite modes

| Symptom | Likely cause | Quick check |
|---------|--------------|-------------|
| `AnalysisException: schema mismatch` | `overwrite` without `overwriteSchema` on changed CSV | Check write options in Bronze/Silver scripts |
| Old bad data still visible | Used `append` instead of `overwrite` | `SELECT COUNT(*), MAX(ingestion_timestamp) FROM bronze_orders` |
| Table disappeared after failed job | Partial write or wrong database context | `SHOW TABLES IN ecommerce_medallion` or `SHOW TABLES` |
| Cannot rollback after bad run | Need time travel | `DESCRIBE HISTORY bronze_orders` then `RESTORE` |

**My notes:**

```
_Add your Delta overwrite issues here._
```

---

### Path issues (local vs DBFS)

| Symptom | Likely cause | Quick check |
|---------|--------------|-------------|
| `FileNotFoundException: customers.csv` | Wrong `BRONZE_DATA_PATH` or not uploaded to CE | `dbutils.fs.ls("/FileStore/data/")` or verify `data/` locally |
| Works locally, fails on Databricks | Hardcoded local path `/workspace/...` | Use env var `BRONZE_DATA_PATH=/FileStore/data` |
| `dbfs:/` path not found | File not copied to workspace | Upload via **Data** → **Add Data** → **Upload File** |
| Different row counts local vs CE | Running stale CSV on one environment | Regenerate: `python src/data_generation/generate_sample_data.py` |

**My notes:**

```
_Add your path issues here._
```

---

## 3. Debugging Approach

Document your step-by-step process for each new issue. Copy this block per incident.

### Template (copy for each issue)

```markdown
#### Issue: [short title] — [date]

**Symptoms:**
- What failed (job name, error message, wrong output)

**Investigation commands:**
```sql
-- Row counts by layer
SELECT 'bronze' AS layer, COUNT(*) FROM bronze_orders
UNION ALL SELECT 'silver', COUNT(*) FROM silver_orders;

SELECT quality_check_result, COUNT(*) FROM silver_orders GROUP BY 1;
```

```python
# PySpark checks
spark.table("silver_orders").filter("customer_id IS NULL").count()
df.groupBy("quality_check_result").count().show()
```

**Diagnosis steps:**
1. _Confirm which layer fails (Bronze / Silver / Gold)_
2. _Compare row counts to expected (110,500 total; ~700 known defects)_
3. _Isolate one failing script (`01_quality_completeness.py`, etc.)_
4. _Check logs / Spark UI for stage failure_

**Cursor / AI usage:**
- Pasted error: `[paste AnalysisException or stack trace]`
- Prompt used: _"Here is the error and file @src/silver/04_quality_referential_integrity.py — root cause and minimal fix?"_
- Useful? _Yes/No — what to verify manually_

**Resolution:**
- _Files changed_
- _Validation run (pytest / manual query)_
```

---

### Example: How I used Cursor to debug

**Scenario A — Delta table merge conflict**

1. Copied full error from notebook output:
   ```
   ConcurrentAppendException: Files were added to the root of the table by a concurrent update ...
   ```
2. In Cursor Chat, referenced `@src/silver/create_silver_tables.py` and the Spark UI timeline (two overlapping jobs).
3. Prompt: *"Silver pipeline fails with ConcurrentAppendException when I run create_silver_tables twice. Minimal fix for CE single-user workflow?"*
4. Verified fix: ran orchestrator once; checked `DESCRIBE HISTORY silver_orders` for a single commit.

**Scenario B — CSV encoding issue**

1. Noticed `customer_name` values like `JosÃ©` after Bronze load.
2. Ran:
   ```sql
   SELECT customer_name FROM bronze_customers WHERE customer_name LIKE '%Ã%' LIMIT 10;
   ```
   Opened source file in a text editor — confirmed UTF-8 vs Latin-1 mismatch.
3. Pasted sample rows + `@src/bronze/01_ingest_customers.py` into Cursor.
4. Prompt: *"Garbled Unicode in bronze_customers after CSV read — encoding option and how to validate?"*

**Scenario C — NULL in aggregation**

1. Noticed dashboard customer metrics with blank `avg_order_value`.
2. Ran:
   ```sql
   SELECT customer_id, total_orders, avg_order_value
   FROM gold_revenue_by_customer
   WHERE avg_order_value IS NULL;
   ```
3. Pasted results + `@src/gold/02_revenue_by_customer.sql` into Cursor.
4. Prompt: *"Why does AVG return NULL here — missing PASS filter or zero-order customers?"*

**My debugging log:**

```
_Add your own scenarios here using the template above._
```

---

## 4. Lessons for Future

### Patterns to avoid

| Avoid | Do instead | Why |
|-------|------------|-----|
| Reading CSV → Gold directly | Always go Bronze → Silver → Gold | Skips DQ; breaks audit trail |
| `filter()` bad rows out in Silver | Flag with `quality_check_result` | Loses row count reconciliation |
| Hardcoded `/Workspace/...` paths | `BRONZE_DATA_PATH` env var or `dbutils` | Breaks on CE vs local |
| `inferSchema=True` without spot checks | Log schema after read; validate types in Silver | Silent STRING dates / amounts |
| Assuming join drops are bugs | Check NULL FKs first (completeness) | NULL ≠ orphan |
| Re-running only Gold after CSV change | Re-run Bronze → Silver → Gold in order | Stale Silver/Gold data |
| Ignoring duplicate `order_id` in sample data | Expect 20 rows in duplicate groups | Uniqueness test assumes known defects |

**My additions:**

```
_
```

---

### Error messages → likely meaning

| Error / message | Layer | Likely meaning | First action |
|-----------------|-------|----------------|--------------|
| `DELTA_CREATE_TABLE_SCHEME_MISMATCH` | Bronze/Silver/Gold | CSV/DataFrame schema ≠ existing Delta table | `overwriteSchema=true` or drop table |
| `AnalysisException: Table or view not found: silver_orders` | Any | Pipeline not run or wrong database | `SHOW TABLES`; run `create_silver_tables.py` |
| `NumberFormatException: For input string: "N/A"` | Silver | Type validation casting bad string | Flag in type check; don't drop row |
| `Py4JJavaError` + `Connection refused` | Local pytest | Spark/Delta jars not loaded | Run on Databricks cluster or install `delta-spark` |
| Gold revenue > Silver PASS sum | Gold | Missing `quality_check_result = 'PASS'` | Fix Gold SQL CTE filter |
| Silver count < Bronze count | Silver | **Critical** — rows deleted (policy violation) | Find `filter()` / `dropDuplicates()` |
| `FAIL: ORPHAN customer_id` spike | Silver | Orphan IDs 99901–99950 in sample data OR real FK issue | `SELECT customer_id, COUNT(*) ... WHERE customer_id >= 99901` |
| Empty dashboard tile | Dashboard | Gold table empty or warehouse stopped | `SELECT COUNT(*) FROM gold_sales_by_product`; start cluster |
| `pytest skip: Delta pipeline unavailable` | Tests | Local env without Delta | Run tests 1–4 locally; 5–6 on Databricks |

**My additions:**

```
_
```

---

## Quick reference commands

```sql
-- Layer row counts
SELECT 'bronze_customers' AS t, COUNT(*) FROM bronze_customers
UNION ALL SELECT 'silver_customers', COUNT(*) FROM silver_customers
UNION ALL SELECT 'bronze_orders', COUNT(*) FROM bronze_orders
UNION ALL SELECT 'silver_orders', COUNT(*) FROM silver_orders;

-- DQ summary
SELECT quality_check_result, COUNT(*) FROM silver_orders GROUP BY 1 ORDER BY 2 DESC;

-- Gold vs Silver reconciliation
SELECT SUM(total_amount) AS silver_pass_revenue
FROM silver_orders WHERE quality_check_result = 'PASS';

SELECT SUM(total_revenue) AS gold_product_revenue FROM gold_sales_by_product;
```

```python
# Databricks notebook — rerun one layer
%run ./src/bronze/ingest_all
%run ./src/silver/create_silver_tables
%run ./src/gold/create_gold_tables
```

```bash
# Local — data quality tests
cd databricks-medallion-pipeline
pytest tests/test_data_quality.py -v
```

---

## Related files

- `data-quality-strategy.md` — expected defect counts (~700 issues)
- `tests/test_data_quality.py` — automated DQ assertions
- `tool-workflow.md` — how to paste errors into Cursor
- `reflection.md` — broader retrospective
