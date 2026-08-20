# Reflection

Personal retrospective on building the e-commerce Medallion pipeline with Cursor AI.  
**Fill in each section honestly** — assessment reviewers care about *how* you used AI, not only what you shipped.

**Related docs:** `tool-workflow.md`, `debugging-notes.md`, `final-ai-usage-summary.md`

---

## 1. What I Built

### Summary of the pipeline

_Describe the end product in 3–5 sentences._

**Guiding questions:**

- What business problem does this pipeline solve (daily e-commerce sales analytics)?
- What are the three source files and approximate row counts (~10K customers, ~100K orders, ~500 products)?
- Which Medallion layers did you implement and where does each live in `src/`?
- What Gold outputs did you deliver (`gold_sales_by_product`, `gold_revenue_by_customer`, `gold_customer_segmentation`)?
- How does the dashboard layer consume Gold (`src/dashboard/dashboard_queries.sql`)?

**My summary:**

```
_
```

---

### What works end-to-end

_List the flows you successfully ran, in order._

**Guiding questions:**

- Can you run `generate_sample_data.py` → Bronze (`ingest_all.py`) → Silver (`create_silver_tables.py`) → Gold (`create_gold_tables.py`) without manual fixes?
- Did you verify row counts at each layer (Bronze = Silver row counts; Gold ≤ Silver PASS rows)?
- Which dashboard visualizations did you actually create on Databricks Community Edition?
- Which pytest tests pass locally (1–4) vs on Databricks (5–6)?
- What did you **not** finish or wire up (e.g. `03_daily_weekly_trends.sql`, `05_quality_business_logic.py`)?

**Checklist (tick what works):**

- [ ] Sample CSV generation (`data/*.csv`)
- [ ] Bronze Delta ingest (all three tables)
- [ ] Silver DQ checks + `silver_quality_report`
- [ ] Gold aggregations (list which tables: ___)
- [ ] Dashboard queries run in SQL editor
- [ ] Automated tests (`pytest tests/test_data_quality.py`)

**Evidence I captured (queries, screenshots, row counts):**

```
_
```

---

## 2. How I Used AI Across the Lifecycle

_For each phase below: what you asked AI to do, what you accepted, what you changed manually._

---

### Requirement analysis

**Guiding questions:**

- Did you start from placeholders (`requirements-analysis.md`) or from scratch with Chat?
- What prompt helped break the problem into Bronze / Silver / Gold tasks?
- Which edge cases did AI surface that you had not considered (NULL FKs, duplicate keys, orphan IDs)?
- Did AI suggest deleting bad rows? How did you redirect to the flag-don't-delete policy?
- Where is the final requirements doc and how much of it is AI-drafted vs your edits?

**Prompt I used (or would use):**

```
> I have three CSVs: customers, orders, products. Orders reference customer_id and product_id.
> List functional requirements for a Medallion pipeline: sales by product, revenue by customer,
> customer segmentation. Group by Bronze, Silver, Gold.
```

**What AI contributed vs what I decided:**

| Topic | AI suggested | My final decision |
|-------|--------------|-------------------|
| DQ policy (flag vs delete) | _ | Flag with `quality_check_result` |
| Sample data volume | _ | 10K / 100K / 500 with ~700 defects |
| Gold table list | _ | _ |
| _ | _ | _ |

**My notes:**

```
_
```

---

### Design

**Guiding questions:**

- Which architecture decisions did you lock in `design-notes.md` before coding?
- Bronze: inferSchema vs explicit schema — what did you choose and why?
- Silver: one monolithic script vs modular `01`–`04` — who proposed what?
- Gold: materialized Delta tables vs views — what trade-off mattered for CE?
- Did AI propose skipping Silver or reading Bronze from Gold? How did you enforce layer boundaries?

**Key design decisions (fill in):**

| Decision | Options AI mentioned | What I chose | Why |
|----------|---------------------|--------------|-----|
| Bronze typing | inferSchema / explicit StructType | _ | _ |
| Silver structure | single script / modular checks | _ | _ |
| Gold delivery | SQL files + orchestrator / views only | _ | _ |
| Segmentation tiers | _ | _ | _ |

**My notes:**

```
_
```

---

### Code generation

**Guiding questions:**

- Which layers did you scaffold with Composer/Agent vs write by hand?
- What repetitive patterns did Tab completion handle (Delta writes, docstrings, CTEs)?
- Did you copy one script as a template (`01_ingest_customers.py` → products/orders)?
- Which files did you rewrite entirely because AI output was wrong?

**Rough estimate (honest ranges are fine):**

| Area | ~% AI-generated | ~% Manual | Notes |
|------|-----------------|-----------|-------|
| Repo scaffold / structure | _ | _ | _ |
| Bronze ingest | _ | _ | _ |
| Silver DQ scripts | _ | _ | _ |
| Gold SQL + Python wrappers | _ | _ | _ |
| Tests (`test_data_quality.py`) | _ | _ | _ |
| Documentation (README, guides) | _ | _ | _ |
| **Overall codebase** | _ | _ | _ |

**Files I mostly wrote myself:**

```
_
```

**Files AI scaffolded and I only edited:**

```
_
```

---

### Testing

**Guiding questions:**

- Did AI help design intentional defects in `generate_sample_data.py`?
- How did AI help write `tests/test_data_quality.py` assertions (expected counts: 50, 100, 20, 50+30)?
- Did AI suggest tests that only cover happy paths? What did you add manually?
- Which tests fail or skip locally and why (Delta / Spark env)?

**Test cases AI helped with:**

```
_
```

**Test cases I wrote or fixed manually:**

```
_
```

**My notes:**

```
_
```

---

### Debugging

**Guiding questions:**

- How many issues did you log in `debugging-notes.md`?
- What errors did you paste into Cursor (schema mismatch, NULL aggregation, encoding)?
- Did AI correctly identify root cause on the first try? If not, what context was missing?
- What did you always verify manually after an AI-suggested fix (re-run pipeline, row counts)?

**Issues where AI helped (link to debugging-notes row #):**

| Issue | AI useful? | What I still verified manually |
|-------|------------|--------------------------------|
| _ | Yes / Partial / No | _ |
| _ | _ | _ |

**My notes:**

```
_
```

---

### Documentation

**Guiding questions:**

- Which docs did AI draft first (`README.md`, `design-notes.md`, `DASHBOARD_GUIDE.md`)?
- What did you edit heavily after generation (accuracy, CE-specific steps, row counts)?
- Did you use AI to keep docs in sync when code changed (schema, DQ metrics)?
- Which doc would be unsafe to ship without human review?

**Docs AI helped create or expand:**

```
_
```

**Docs I wrote mostly myself:**

```
_
```

**My notes:**

```
_
```

---

## 3. What AI Helped With Most

**Guiding questions:**

- Where did AI save the most *calendar* time vs *thinking* time?
- Which tasks would have taken longest without Composer multi-file edits?
- Did AI help most with boilerplate, SQL CTEs, or explaining Spark errors?
- What would you still do yourself even with AI available?

**Top 3 areas (ranked):**

| Rank | Area | Why it saved time | Example from this project |
|------|------|-------------------|---------------------------|
| 1 | _ | _ | e.g. scaffolding five Silver DQ modules from `data-quality-strategy.md` |
| 2 | _ | _ | e.g. Gold SQL with PASS filters and CTE structure |
| 3 | _ | _ | e.g. README + dashboard guide from existing code |

**Estimated time saved (rough):**

```
_
```

---

## 4. What AI Got Wrong

**Guiding questions:**

- Did AI suggest `filter()` or `dropDuplicates()` that violated DQ policy?
- Did it use wrong table names (`bronze.orders` vs `bronze_orders`)?
- Did it skip `quality_check_result = 'PASS'` in Gold SQL?
- Did it generate uniqueness logic that only flagged the second duplicate row?
- Did it hallucinate Databricks features not available on Community Edition?

**Incident log (add one row per mistake):**

### Example A — _[title]_

| Field | Your notes |
|-------|------------|
| **What AI produced** | _e.g. "Remove rows where customer_id IS NULL before Silver write"_ |
| **Why it was wrong** | _Violates flag-don't-delete; breaks row count reconciliation_ |
| **How I spotted it** | _Row count dropped; or read `.cursorrules` §8; or pytest failed_ |
| **How I fixed it** | _Replaced filter with `quality_check_result = 'FAIL_COMPLETENESS'` flag_ |

### Example B — _[title]_

| Field | Your notes |
|-------|------------|
| **What AI produced** | _ |
| **Why it was wrong** | _ |
| **How I spotted it** | _ |
| **How I fixed it** | _ |

### Example C — _[title]_

| Field | Your notes |
|-------|------------|
| **What AI produced** | _ |
| **Why it was wrong** | _ |
| **How I spotted it** | _ |
| **How I fixed it** | _ |

**Patterns I now watch for in AI output:**

```
- Silent row removal (filter, drop, dropDuplicates)
- Gold reading Bronze directly
- Missing docstrings / row count logging
- Wrong duplicate-key handling (second row only)
- Over-engineered abstractions for simple PySpark scripts
```

---

## 5. How I Validated AI Output

**Guiding questions:**

- What is your default loop: generate → run → compare counts → read code → accept/reject?
- Which SQL spot checks do you always run after Gold changes?
- When do you reject AI suggestions entirely vs patch minimally?
- Did you use pytest, manual notebooks, or both?

### My validation process

```
1. RUN     → Execute the smallest scope (one script, not full pipeline)
2. CHECK   → Row counts, schema, quality_check_result distribution
3. VERIFY  → Compare to expected metrics in data-quality-strategy.md
4. DECIDE  → Accept, patch manually, or reject and re-prompt with constraints
```

**Fill in your version:**

| Step | What I do | Tools / commands |
|------|-----------|------------------|
| Run | _ | `%run`, `pytest`, `spark.table(...).count()` |
| Check | _ | `GROUP BY quality_check_result`, `DESCRIBE TABLE` |
| Verify | _ | Spreadsheet sum, test assertions, design doc |
| Accept/Reject | _ | _ |

### Examples of rejected suggestions

**Guiding questions:**

- What AI suggestion did you refuse even though it "looked clean"?
- Did you reject a full rewrite in favor of a one-line fix?
- Did you reject deleting a table / dropping Delta history?

| # | AI suggestion | Why I rejected | What I did instead |
|---|---------------|----------------|---------------------|
| 1 | _e.g. separate quarantine tables only_ | _Loses audit trail in Silver_ | _Keep all rows with flags_ |
| 2 | _ | _ | _ |
| 3 | _ | _ | _ |

**My notes:**

```
_
```

---

## 6. What I Would Improve Next

**Guiding questions:**

- What is missing from a production-ready pipeline (monitoring, incremental loads, idempotency keys)?
- Which scaffold files should be finished (`03_daily_weekly_trends`, `05_quality_business_logic`)?
- Would you add Unity Catalog, Delta Live Tables, or streaming?
- What would you do differently in `.cursorrules` or prompts knowing what AI got wrong?
- What testing gap remains (integration tests on CE, data contract tests)?

### If I had more time, I would add

```
-
-
-
```

### What I would do differently

| Area | What I did | What I'd change |
|------|------------|-----------------|
| Prompting | _ | _e.g. always @-mention `.cursorrules` + one reference script_ |
| Design | _ | _ |
| Testing | _ | _ |
| Documentation | _ | _ |
| AI usage | _ | _ |

**My notes:**

```
_
```

---

## 7. Reusable Workflow

**Guiding questions:**

- Which habits from this project would you reuse on a real Databricks engagement?
- What belongs in `.cursorrules` vs per-task prompts?
- Which reference files do you always attach at the start of a new chat?
- When do you switch from Chat (design) to Composer (implement)?

### Patterns I'd use again in production

```
1. Layer-scoped prompts — never "build the whole pipeline" in one shot
2. Flag-don't-delete encoded in .cursorrules before any Silver code
3. Numbered scripts + orchestrators (ingest_all, create_silver_tables, create_gold_tables)
4. Row count logging at every read/write boundary
5. debugging-notes.md issue log for recurring CE limitations
6. Reference-file chaining: @design-notes.md + @01_ingest_customers.py as template
```

**My additions:**

```
_
```

### Prompt templates worth keeping

Copy proven prompts to `ai-prompts/` or your team wiki.

#### Template: New Bronze ingest script

```
Implement src/bronze/0X_ingest_<entity>.py:
- Read data/<entity>.csv with header, inferSchema=True
- Add ingestion_timestamp and source_file
- Write Delta table bronze_<entity>, mode overwrite, overwriteSchema=true
- Module docstring, type hints, try/except, logging, row counts
- Follow @.cursorrules — Databricks Community Edition compatible
- Match pattern in @src/bronze/01_ingest_customers.py
```

#### Template: Silver DQ check

```
Implement src/silver/0X_quality_<check>.py for <table>:
- Flag failures in quality_<check> column; set quality_check_result to FAIL_<CODE>
- Never filter or drop rows
- Log pass/fail counts
- Align with @data-quality-strategy.md Check N
- Follow @.cursorrules
```

#### Template: Debug with context

```
Error when running <script> on Databricks CE:
<paste full stack trace>

Row counts: bronze_orders=<n>, silver_orders=<n>
Files: @<script> @.cursorrules

Root cause and minimal fix — do not drop rows or skip Silver layer.
```

**Prompts that worked well on this project (my versions):**

```
_
```

**Prompts I would retire or rewrite:**

```
_
```

---

## Closing

**One sentence — what I learned about AI-assisted data engineering:**

```
_
```

**Confidence level in running this pipeline without AI next time (1–5):** _  
**Confidence level in using AI effectively on the next pipeline (1–5):** _

---

**Author:** _Your name_  
**Date completed:** _YYYY-MM-DD_
