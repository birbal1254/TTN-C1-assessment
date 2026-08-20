# Project Context — Cursor Setup

How persistent context is configured in Cursor for the **e-commerce Medallion pipeline** (`databricks-medallion-pipeline/`).  
Use this doc when onboarding to the repo or starting a new agent session.

**Related:** `.cursorrules`, `tool-workflow.md`, `ai-prompts/`

---

## 1. `.cursorrules` File

**Location:** `databricks-medallion-pipeline/.cursorrules`

Cursor loads this automatically for every Chat, Composer, and Agent session in the project. It is the **highest-priority constraint** — more reliable than repeating rules in each prompt.

### Rules defined and why

| Rule area | What `.cursorrules` enforces | Why it matters for this project |
|-----------|------------------------------|----------------------------------|
| **Language & runtime** | Python 3.9+, PySpark, SQL; Databricks **Community Edition** only | Avoids paid-tier APIs (some Unity Catalog patterns, proprietary connectors) |
| **Architecture** | CSV → Bronze → Silver → Gold → Dashboard; no layer skipping | AI often shortcuts CSV → Gold; rule prevents audit/DQ gaps |
| **Naming** | `bronze_*`, `silver_*`, `gold_*` tables; `snake_case` functions | Keeps Delta tables, SQL, and dashboard queries consistent |
| **Script structure** | Module docstring, type hints, try/except, logging, **row counts** | CE debugging relies on logged counts; AI often omits docstrings |
| **DQ policy** | **Never delete bad rows** — `quality_check_result` flags | #1 AI mistake on this project is `filter()` / `dropDuplicates()` |
| **SQL style** | CTEs, clear aliases, Gold reads Silver only, PASS filter | Readable Gold SQL; prevents Bronze leakage into analytics |
| **Data format** | CSV in `data/`; Delta out; Bronze infers, Silver validates types | Matches sample-data + assessment scope |
| **Comments** | Explain *why*, not obvious *what* | Stops redundant `# read csv` noise from AI |

### Example: rule preventing a common AI mistake

**Without `.cursorrules`**, AI often generates:

```python
df = df.filter(col("customer_id").isNotNull())  # "clean" orders
```

**With `.cursorrules` §8**, AI should generate:

```python
df = df.withColumn(
    "quality_check_result",
    when(col("customer_id").isNull(), lit("FAIL_COMPLETENESS")).otherwise(col("quality_check_result"))
)
```

### When to update `.cursorrules`

- New global convention (e.g. add `ecommerce_medallion` database prefix)
- Repeated AI mistake appears twice → encode as a rule
- Assessment policy change (e.g. new required Gold table)

**Do not** put task-specific instructions in `.cursorrules` (e.g. "fix line 45 today") — use Chat prompts instead.

---

## 2. Reference Files

Keep these available via `@` mentions or pinned tabs. Order matters: **rules → design → reference implementation → task file**.

### Tier 1 — Always attach for new features

| File | Role in context |
|------|-----------------|
| `@.cursorrules` | Non-negotiable conventions |
| `@design-notes.md` | Layer responsibilities, flow, table list |
| `@data-quality-strategy.md` | DQ checks, expected defect counts, flag values |

### Tier 2 — Layer-specific work

| Task | Reference files |
|------|-----------------|
| Bronze ingest | `@src/bronze/01_ingest_customers.py`, `@src/bronze/bronze_helpers.py` |
| Silver DQ | `@src/silver/01_quality_completeness.py`, `@data-quality-strategy.md` |
| Gold SQL | `@src/gold/01_sales_by_product.sql`, `@design-notes.md` |
| Dashboard | `@src/dashboard/dashboard_queries.sql`, `@src/dashboard/DASHBOARD_GUIDE.md` |
| Tests | `@tests/test_data_quality.py`, `@tests/conftest.py` |

### Tier 3 — Requirements and debugging

| File | When to use |
|------|-------------|
| `@requirements-analysis.md` | Scoping, acceptance criteria |
| `@data-model.md` | Column names, FK relationships |
| `@debugging-notes.md` | Recurring errors, CE limitations |
| `@tool-specific/cursor-workflow/spec.md` | Scope boundary for agent tasks |

### Example `@` prompt from this project

```
Read @data-model.md and @design-notes.md.
Implement @src/bronze/02_ingest_orders.py using the same pattern as
@src/bronze/01_ingest_customers.py.
Follow @.cursorrules — add ingestion_timestamp, source_file, Delta overwrite.
```

### Files I keep open (pinned tabs)

Typical layout during Silver work:

1. `.cursorrules`
2. `data-quality-strategy.md`
3. Active script (e.g. `04_quality_referential_integrity.py`)
4. `create_silver_tables.py` (orchestrator)
5. `tests/test_data_quality.py` (expected counts)

---

## 3. Conversation Strategy

**Principle:** Start every new Chat with **scope + constraints + references** — not "build my pipeline."

### How I start new chats

**Step 1 — State the layer and single task**

```
Task: Implement Silver referential integrity for orders only.
Do not modify Gold or Bronze ingest scripts.
```

**Step 2 — Attach schema + requirements**

```
Context: @.cursorrules @data-quality-strategy.md Check 4
@data-model.md (orders.customer_id → customers, orders.product_id → products)
```

**Step 3 — Attach a pattern file**

```
Match flagging style in @src/silver/01_quality_completeness.py
```

**Step 4 — State acceptance criteria**

```
Done when:
- 50 orphan customer_id and 30 orphan product_id rows flagged in sample data
- No row count change vs bronze_orders
- pytest test_referential_integrity_catches_orphans passes
```

### New chat vs continue thread

| Situation | Strategy |
|-----------|----------|
| New layer (Bronze → Silver) | **New chat** — fresh context, attach tier-1 files |
| Same layer, next numbered script | **Continue** if thread < ~20 turns; else new chat with `@01_*` as template |
| Debugging one error | **New chat** — paste stack trace + 2 files only |
| Large doc generation (README) | **New chat** — `@README.md` + `@design-notes.md`, ask for one section at a time |

### What I always provide (schema + requirements)

Minimal context block (copy/paste and edit):

```
Project: Databricks Medallion e-commerce pipeline (Community Edition)
Sources: data/customers.csv (10K), orders.csv (100K), products.csv (500)
Policy: Flag bad rows in Silver — never delete. Gold uses PASS only.
Tables: bronze_* → silver_* → gold_*
Task: [one sentence]
Files: @[relevant docs and reference script]
```

---

## 4. Context Window Management

Long threads degrade quality — AI forgets `.cursorrules` and repeats mistakes.

### Practices for this repo

| Technique | Example |
|-----------|---------|
| **One layer per session** | Session A: all Bronze scripts. Session B: Silver 01–04. Session C: Gold. |
| **Summarize before continuing** | "We decided: Bronze uses inferSchema; Silver enforces types. Next: uniqueness check only." |
| **Re-attach rules mid-thread** | "@.cursorrules reminder: do not filter null customer_id rows" |
| **Split large requests** | Instead of "implement all Gold," do one SQL file per Composer run |
| **Close with a handoff note** | Update `debugging-notes.md` or `task-breakdown.md` so the next chat does not re-discover the same issue |

### Signs you need a new chat

- AI suggests deleting DQ failures again after you corrected it once
- Generated code uses wrong table names (`bronze.orders`)
- Responses get generic (no longer reference your file paths)
- Thread includes unrelated tasks (README + Silver + debugging)

### Context budget priority (when trimming)

1. `.cursorrules` (never drop)
2. Active file + one reference file
3. `data-quality-strategy.md` or `design-notes.md` (pick one)
4. Paste error message / row counts (for debugging)

Drop first: long conversation history about completed layers.

---

## 5. Composer vs Chat

| Tool | Best for | This project — examples |
|------|----------|-------------------------|
| **Chat** | Questions, trade-offs, explaining errors, reviewing design | "Should type validation live in Bronze or Silver?" / paste `AnalysisException` |
| **Composer / Agent** | Multi-file implementation, scaffolding, doc batches | Generate all three Bronze ingest scripts + update `ingest_all.py` |
| **Inline Edit (Cmd+K)** | Small localized change | Fix one Gold CTE filter; add `overwriteSchema` to one write |
| **Agent (Cloud)** | Full repo tasks with git/PR | Scaffold entire Medallion structure, README, tests |

### Decision flow

```
Is it a "why" or "what if" question?
  └─ Yes → Chat

Does it touch 2+ files or need new files?
  └─ Yes → Composer / Agent

Is it a 5–20 line fix in one file?
  └─ Yes → Inline Edit or Tab completion

Does it need run/test/commit loop?
  └─ Yes → Agent (with explicit test commands)
```

### Concrete examples from this project

| Task | Tool used | Why |
|------|-----------|-----|
| Break requirements into Bronze/Silver/Gold FRs | **Chat** | No code change; output → `requirements-analysis.md` |
| Scaffold repo + Bronze/Silver/Gold folders | **Agent** | Many files, one coherent structure |
| Implement `02_quality_uniqueness.py` only | **Composer** | One module + maybe test update |
| Explain Delta schema mismatch error | **Chat** | Paste error + `@02_ingest_orders.py` |
| Fix duplicate-order flag to include all rows in group | **Inline Edit** | Single function change after test failure |
| Write comprehensive README | **Agent** | Pull from multiple docs; single artifact |

---

## 6. Tab Completion

Tab completion excels at **repetitive patterns** once one file establishes the convention.

### Patterns I accept from Tab on this project

| Pattern | Trigger context | Example completion |
|---------|-----------------|-------------------|
| Delta write block | After `df.write` in Bronze | `.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_orders")` |
| Logging + row count | After `df.count()` | `logger.info("Read %d rows from %s", count, source_path)` |
| Google-style docstring | After `def ingest_` | Args/Returns blocks matching `.cursorrules` |
| SQL CTE skeleton | `WITH valid_orders AS (` | PASS filter from `silver_orders` |
| Quality flag column | `quality_check_result` | `when(..., lit("FAIL_COMPLETENESS")).otherwise(...)` |
| pytest assertion message | `assert count ==` | `f"Expected 50 NULL email flags, found {count}"` |

### When I do **not** trust Tab completion

- Business logic (segmentation tiers, revenue buckets) — verify against `design-notes.md`
- Join keys in referential integrity — Tab may guess wrong parent table
- `filter()` / `drop()` lines — always read; Tab may "clean" data against DQ policy
- Import paths in numbered scripts (`01_`, `02_`) — may break Databricks `%run` layout

### Workflow: template file + Tab

1. Hand-write or AI-generate **`01_ingest_customers.py`** correctly.
2. Open **`03_ingest_products.py`** — type function signature and module docstring stem.
3. Use Tab to fill read/write/logging blocks; **manually change** table names and paths.
4. Run row count check before moving to next entity.

Same pattern for Silver: complete `01_quality_completeness.py`, then Tab-assist `02_quality_uniqueness.py`.

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────┐
│  NEW TASK IN CURSOR                                         │
├─────────────────────────────────────────────────────────────┤
│  1. @.cursorrules                                           │
│  2. @design-notes.md OR @data-quality-strategy.md           │
│  3. @src/<layer>/<reference_script>                         │
│  4. One-sentence task + acceptance criteria                 │
│  5. Chat (design) → Composer (code) → run → pytest/SQL      │
│  6. Log mistakes in debugging-notes.md                      │
└─────────────────────────────────────────────────────────────┘
```

---

## Related files in this folder

| File | Purpose |
|------|---------|
| `spec.md` | Scope, inputs/outputs, acceptance criteria |
| `task-breakdown.md` | Layer-by-layer task checklist |
| `cursor-rules-or-instructions.md` | Extended Cursor usage notes |

**Last updated:** August 2026
