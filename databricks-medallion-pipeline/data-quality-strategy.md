# Data Quality Strategy

Silver-layer data quality rules for the e-commerce Medallion pipeline. All checks run in `src/silver/` and align with `design-notes.md` and `.cursorrules`.

**Core policy:** Flag bad rows—**never delete**. Every row lands in Silver with a `quality_check_result` value. Gold consumes **PASS rows only**.

---

## Sample Data Profile

Intentional defects for testing (~**700 problematic row-checks** across ~**110,500** total rows ≈ **0.7%** failure rate at check level).

| File | Row count | Intentional issues |
|------|-----------|-------------------|
| `customers.csv` | 10,000 | 50 NULL `email`; 10 rows involved in duplicate `customer_id` |
| `orders.csv` | 100,000 | 100 NULL `customer_id`; 200 NULL `product_id`; 50 orphan `customer_id`; 30 orphan `product_id`; 20 rows involved in duplicate `order_id` |
| `products.csv` | 500 | None (clean catalog) |

**Note:** One physical row can fail multiple checks (e.g. NULL `customer_id` fails completeness and skips referential match). Metrics below are reported **per check**; overall `quality_check_result = 'PASS'` requires all checks to pass.

---

## Quality Check Execution Order

Checks accumulate into `quality_check_result` in this order:

```
1. Completeness   (01_quality_completeness.py)
2. Uniqueness     (02_quality_uniqueness.py)
3. Type validation (03_quality_type_validation.py)
4. Referential integrity (04_quality_referential_integrity.py)
```

If a row already failed an earlier check, later checks still run for **metrics reporting**, but the final flag reflects the **first failure** or a **composite** code (see § Flagging Strategy).

---

## Check 1: Completeness

**Script:** `src/silver/01_quality_completeness.py`

### What it checks

Required columns must be non-null and non-empty (after trim).

| Table | Required columns |
|-------|------------------|
| `silver_customers` | `customer_id`, `customer_name`, `email`, `country`, `signup_date` |
| `silver_orders` | `order_id`, `customer_id`, `product_id`, `order_date`, `quantity`, `unit_price`, `total_amount`, `order_status` |
| `silver_products` | `product_id`, `product_name`, `category`, `price` |

### Detection logic

**PySpark:**

```python
from pyspark.sql import functions as F

REQUIRED = {
    "silver_customers": ["customer_id", "customer_name", "email", "country", "signup_date"],
    "silver_orders": [
        "order_id", "customer_id", "product_id", "order_date",
        "quantity", "unit_price", "total_amount", "order_status",
    ],
    "silver_products": ["product_id", "product_name", "category", "price"],
}

def completeness_expr(columns: list) -> F.Column:
    """True when every required column is non-null and non-blank."""
    conditions = [
        F.col(c).isNotNull() & (F.trim(F.col(c).cast("string")) != "")
        for c in columns
    ]
    return conditions[0] if len(conditions) == 1 else F.reduce(
        lambda a, b: a & b, conditions
    )

# Apply per table
df = df.withColumn(
    "completeness_pass",
    completeness_expr(REQUIRED["silver_customers"]),
)
```

**SQL (single-column example — `email` on customers):**

```sql
SELECT
  customer_id,
  email,
  CASE
    WHEN email IS NULL OR TRIM(CAST(email AS STRING)) = ''
      THEN FALSE
    ELSE TRUE
  END AS email_complete
FROM bronze_customers;
```

### Pass / fail threshold

| Threshold | Value |
|-----------|-------|
| **Pass** | 100% of required fields populated on the row |
| **Fail** | Any required field NULL or blank |
| **Alert threshold** | Any check with pass rate < **99.0%** triggers operator review (sample data expects ~99.5%+ on most columns) |

### Flagging (`quality_check_result`)

| Condition | Flag |
|-----------|------|
| All required fields present | No change (remains `PASS` or prior state) |
| Any required field missing | `FAIL_COMPLETENESS` |

Optional detail column: `completeness_failed_columns` = comma-separated list (e.g. `email`, `customer_id`).

### Expected metrics (sample data)

| Table | Column / check | Total rows | Failed | Passed | Pass rate |
|-------|----------------|------------|--------|--------|-----------|
| `silver_customers` | `email` | 10,000 | 50 | 9,950 | **99.5%** |
| `silver_customers` | all required fields | 10,000 | 50 | 9,950 | **99.5%** |
| `silver_orders` | `customer_id` | 100,000 | 100 | 99,900 | **99.9%** |
| `silver_orders` | `product_id` | 100,000 | 200 | 99,800 | **99.8%** |
| `silver_orders` | all required fields | 100,000 | ≤300* | ≥99,700 | **≥99.7%** |
| `silver_products` | all required fields | 500 | 0 | 500 | **100.0%** |

\*Up to 300 if NULL `customer_id` and NULL `product_id` sets are disjoint; fewer if some rows fail both.

---

## Check 2: Uniqueness

**Script:** `src/silver/02_quality_uniqueness.py`

### What it checks

Primary key must be unique within each entity table.

| Table | Primary key |
|-------|-------------|
| `silver_customers` | `customer_id` |
| `silver_orders` | `order_id` |
| `silver_products` | `product_id` |

**Rule:** Flag **every row** that shares a duplicated key—not only the “second” occurrence.

### Detection logic

**PySpark:**

```python
from pyspark.sql import Window
import pyspark.sql.functions as F

def flag_duplicate_keys(df, key_col: str) -> "DataFrame":
    w = Window.partitionBy(key_col)
    return df.withColumn(
        "uniqueness_pass",
        F.count(F.lit(1)).over(w) == 1,
    )
```

**SQL:**

```sql
WITH keyed AS (
  SELECT
    customer_id,
    customer_name,
    COUNT(*) OVER (PARTITION BY customer_id) AS key_count
  FROM bronze_customers
)
SELECT
  *,
  key_count = 1 AS uniqueness_pass
FROM keyed;
```

### Pass / fail threshold

| Threshold | Value |
|-----------|-------|
| **Pass** | `key_count = 1` for the row's primary key |
| **Fail** | `key_count > 1` (all rows with that key fail) |
| **Alert threshold** | Pass rate < **99.9%** on primary keys |

### Flagging (`quality_check_result`)

| Condition | Flag |
|-----------|------|
| Unique primary key | No change |
| Duplicate primary key | `FAIL_UNIQUENESS` |

### Expected metrics (sample data)

| Table | Key | Total rows | Failed | Passed | Pass rate |
|-------|-----|------------|--------|--------|-----------|
| `silver_customers` | `customer_id` | 10,000 | 10 | 9,990 | **99.9%** |
| `silver_orders` | `order_id` | 100,000 | 20 | 99,980 | **99.98%** |
| `silver_products` | `product_id` | 500 | 0 | 500 | **100.0%** |

**Note:** The 10 duplicate customer rows may overlap with the 50 NULL-email rows; overall customer `PASS` count will be lower than `10,000 - 60`.

---

## Check 3: Type Validation

**Script:** `src/silver/03_quality_type_validation.py`

### What it checks

Values must match expected types and formats after Bronze inference and Silver casting.

| Table | Column | Expected type / rule |
|-------|--------|----------------------|
| `silver_customers` | `signup_date` | Valid date |
| `silver_customers` | `email` | Contains `@` and domain (basic regex) |
| `silver_customers` | `lifetime_value` | Numeric if present |
| `silver_orders` | `order_date`, `payment_date` | Valid date (payment_date nullable) |
| `silver_orders` | `quantity` | Integer ≥ 1 |
| `silver_orders` | `unit_price`, `total_amount` | Decimal ≥ 0 |
| `silver_products` | `price`, `cost` | Decimal ≥ 0 |
| `silver_products` | `stock_quantity`, `reorder_level` | Integer ≥ 0 if present |

### Detection logic

**PySpark:**

```python
import pyspark.sql.functions as F

email_valid = F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
date_valid = F.to_date(F.col("signup_date")).isNotNull()
numeric_valid = F.col("total_amount").cast("double").isNotNull()

df = df.withColumn(
    "type_validation_pass",
    email_valid & date_valid & numeric_valid,  # extend per column
)
```

**SQL (orders numeric example):**

```sql
SELECT
  order_id,
  TRY_CAST(total_amount AS DOUBLE) IS NOT NULL AS amount_is_numeric,
  TRY_CAST(quantity AS INT) IS NOT NULL AND CAST(quantity AS INT) >= 1 AS quantity_valid
FROM bronze_orders;
```

### Pass / fail threshold

| Threshold | Value |
|-----------|-------|
| **Pass** | All typed columns parse and satisfy domain rules |
| **Fail** | Any column fails cast or format rule |
| **Alert threshold** | Pass rate < **99.5%** |

### Flagging (`quality_check_result`)

| Condition | Flag |
|-----------|------|
| All types valid | No change |
| Invalid type or format | `FAIL_TYPE_VALIDATION` |

### Expected metrics (sample data)

| Table | Check | Total rows | Failed | Passed | Pass rate |
|-------|-------|------------|--------|--------|-----------|
| `silver_customers` | all type rules | 10,000 | ~0* | ~10,000 | **~100.0%** |
| `silver_orders` | all type rules | 100,000 | ~0* | ~100,000 | **~100.0%** |
| `silver_products` | all type rules | 500 | 0 | 500 | **100.0%** |

\*Sample data injects nulls and FK issues, not type corruption. Failures appear only if NULL-email rows also have invalid formats.

---

## Check 4: Referential Integrity

**Script:** `src/silver/04_quality_referential_integrity.py`

### What it checks

Foreign keys on orders must reference existing parent keys in the **same batch** (Silver customer and product tables).

| Child | FK column | Parent table | Parent key |
|-------|-----------|--------------|------------|
| `silver_orders` | `customer_id` | `silver_customers` | `customer_id` |
| `silver_orders` | `product_id` | `silver_products` | `product_id` |

**Rules:**

- Evaluate only rows where FK is **non-null** (null FKs are completeness failures).
- Parent match uses **all** Silver parent rows (including those that fail other checks), so orphan detection reflects missing keys in the daily extract—not PASS-only parents.

### Detection logic

**PySpark:**

```python
from pyspark.sql import functions as F

customers = spark.table("silver_customers").select("customer_id").distinct()
products = spark.table("silver_products").select("product_id").distinct()

orders = spark.table("bronze_orders")

validated = (
    orders
    .join(customers, on="customer_id", how="left")
    .withColumn("customer_exists", F.col("customer_id").isNull() | customers["customer_id"].isNotNull())
    .drop(customers["customer_id"])
    .join(products, on="product_id", how="left")
    .withColumn(
        "referential_pass",
        (
            F.col("customer_id").isNull()
            | F.col("customer_id").isin([r.customer_id for r in customers.collect()])  # use join flag in practice
        )
        & (
            F.col("product_id").isNull()
            | F.col("product_id").isin([r.product_id for r in products.collect()])
        ),
    )
)
```

**SQL (preferred — scalable):**

```sql
WITH valid_customers AS (
  SELECT DISTINCT customer_id FROM silver_customers
),
valid_products AS (
  SELECT DISTINCT product_id FROM silver_products
)
SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  CASE
    WHEN o.customer_id IS NOT NULL
     AND vc.customer_id IS NULL THEN FALSE
    WHEN o.product_id IS NOT NULL
     AND vp.product_id IS NULL THEN FALSE
    ELSE TRUE
  END AS referential_pass
FROM bronze_orders AS o
LEFT JOIN valid_customers AS vc ON o.customer_id = vc.customer_id
LEFT JOIN valid_products AS vp ON o.product_id = vp.product_id;
```

### Pass / fail threshold

| Threshold | Value |
|-----------|-------|
| **Pass** | Non-null FK exists in parent table |
| **Fail** | Non-null FK missing from parent table |
| **Alert threshold** | Pass rate < **99.9%** on each FK |

### Flagging (`quality_check_result`)

| Condition | Flag |
|-----------|------|
| All FKs resolve | No change |
| Orphan `customer_id` and/or `product_id` | `FAIL_REFERENTIAL_INTEGRITY` |

Optional detail: `referential_failure_detail` = `MISSING_CUSTOMER`, `MISSING_PRODUCT`, or `BOTH`.

### Expected metrics (sample data)

| Table | FK check | Total rows | Failed | Passed | Pass rate |
|-------|----------|------------|--------|--------|-----------|
| `silver_orders` | `customer_id` → customers | 100,000 | 50* | 99,950 | **99.95%** |
| `silver_orders` | `product_id` → products | 100,000 | 30* | 99,970 | **99.97%** |
| `silver_orders` | both FKs | 100,000 | ≤80* | ≥99,920 | **≥99.92%** |

\*Orphan counts assume non-null FKs. NULL FK rows fail completeness, not referential. Overlap between orphan customer and orphan product on same row reduces distinct failure count.

---

## Flagging Strategy (`quality_check_result`)

### Column definition

| Column | Type | Description |
|--------|------|-------------|
| `quality_check_result` | string | Final outcome after all checks |
| `quality_failure_reasons` | string (optional) | Comma-separated list of failed checks for auditing |

### Flag values

| Value | Set when |
|-------|----------|
| `PASS` | All four checks pass |
| `FAIL_COMPLETENESS` | Missing required field(s) |
| `FAIL_UNIQUENESS` | Duplicate primary key |
| `FAIL_TYPE_VALIDATION` | Invalid type or format |
| `FAIL_REFERENTIAL_INTEGRITY` | Orphan foreign key |
| `FAIL_MULTIPLE:COMPLETENESS,REFERENTIAL_INTEGRITY` | Multiple failures (optional composite) |

### Priority when multiple checks fail

For **metrics**, each check reports independently. For the **single** `quality_check_result` column, use either:

1. **First failure wins** (check order 1→4), or  
2. **Composite code** `FAIL_MULTIPLE:...` (recommended for audit clarity)

**Rows are never removed from Silver** regardless of flag.

### Example rows (orders)

| order_id | Issue | `quality_check_result` |
|----------|-------|------------------------|
| ORD-001 | Valid | `PASS` |
| ORD-002 | NULL `customer_id` | `FAIL_COMPLETENESS` |
| ORD-003 | `customer_id` not in customers | `FAIL_REFERENTIAL_INTEGRITY` |
| ORD-004 | Duplicate `order_id` | `FAIL_UNIQUENESS` |

---

## Quarantine Handling — Flagged, Not Deleted

This pipeline does **not** use a separate quarantine table that drops rows from Silver.

| Approach | Behavior |
|----------|----------|
| **In-place flagging** | Every Bronze row copied to Silver with `quality_check_result` |
| **Row retention** | `COUNT(silver_*) = COUNT(bronze_*)` per entity |
| **Audit** | Optional view `silver_orders_quarantine` = `SELECT * FROM silver_orders WHERE quality_check_result != 'PASS'` |
| **No silent drops** | Forbidden: `df.filter(col("quality_check_result") == "PASS")` before writing Silver |

**Why flag instead of quarantine-delete:** Preserves full daily volume for reconciliation with source systems; supports reprocessing when upstream fixes data.

---

## Downstream Layer Strategy

### Silver → Gold

Gold reads **only** rows where `quality_check_result = 'PASS'`:

```sql
WITH valid_orders AS (
  SELECT *
  FROM silver_orders
  WHERE quality_check_result = 'PASS'
),
valid_customers AS (
  SELECT *
  FROM silver_customers
  WHERE quality_check_result = 'PASS'
),
valid_products AS (
  SELECT *
  FROM silver_products
  WHERE quality_check_result = 'PASS'
)
SELECT ...
FROM valid_orders AS o
INNER JOIN valid_products AS p ON o.product_id = p.product_id;
```

| Layer | Uses failed rows? | Rule |
|-------|-------------------|------|
| Bronze | N/A | All source rows landed |
| Silver | Stores all rows | Failures flagged |
| Gold | **No** | PASS-only joins and aggregations |
| Dashboard | **No** | Queries Gold tables built from PASS data |

### Expected overall PASS rates (sample data)

Approximate rows passing **all** checks (accounting for overlaps):

| Table | Total rows | Est. all-check PASS | Est. overall pass rate |
|-------|------------|---------------------|------------------------|
| `silver_customers` | 10,000 | ~9,940 | **~99.4%** |
| `silver_orders` | 100,000 | ~99,530 | **~99.5%** |
| `silver_products` | 500 | 500 | **100.0%** |
| **All tables** | **110,500** | **~109,970** | **~99.4%** (~530 distinct bad rows; ~700 check-level failures when counting multi-failures per row) |

If **all** order rows failed DQ, Gold tables would return **zero rows** but the pipeline would still **complete successfully** with a logged warning.

---

## Quality Metrics Report

Generated at the end of `create_silver_tables.py` and written to Delta table **`silver_quality_metrics`**.

### Report schema

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | UUID or timestamp id for the pipeline run |
| `run_timestamp` | timestamp | When checks completed |
| `table_name` | string | e.g. `silver_orders` |
| `check_name` | string | e.g. `completeness`, `uniqueness`, `type_validation`, `referential_integrity`, `overall_pass` |
| `column_name` | string | Optional; e.g. `email`, `customer_id` FK |
| `total_rows` | long | Rows evaluated |
| `passed` | long | Rows passing this check |
| `failed` | long | Rows failing this check |
| `pass_rate` | double | `passed / total_rows` (0–1) |

### Example report (sample data)

| table_name | check_name | column_name | total_rows | passed | failed | pass_rate |
|------------|------------|-------------|------------|--------|--------|-----------|
| silver_customers | completeness | email | 10,000 | 9,950 | 50 | 0.9950 |
| silver_customers | completeness | all_required | 10,000 | 9,950 | 50 | 0.9950 |
| silver_customers | uniqueness | customer_id | 10,000 | 9,990 | 10 | 0.9990 |
| silver_customers | type_validation | all | 10,000 | 10,000 | 0 | 1.0000 |
| silver_customers | overall_pass | — | 10,000 | 9,940 | 60 | 0.9940 |
| silver_orders | completeness | customer_id | 100,000 | 99,900 | 100 | 0.9990 |
| silver_orders | completeness | product_id | 100,000 | 99,800 | 200 | 0.9980 |
| silver_orders | uniqueness | order_id | 100,000 | 99,980 | 20 | 0.9998 |
| silver_orders | type_validation | all | 100,000 | 100,000 | 0 | 1.0000 |
| silver_orders | referential_integrity | customer_id | 100,000 | 99,950 | 50 | 0.9995 |
| silver_orders | referential_integrity | product_id | 100,000 | 99,970 | 30 | 0.9997 |
| silver_orders | overall_pass | — | 100,000 | 99,530 | 470 | 0.9953 |
| silver_products | overall_pass | — | 500 | 500 | 0 | 1.0000 |

### PySpark snippet to build the report

```python
from pyspark.sql import Row

def emit_metric(table_name: str, check_name: str, total: int, passed: int, column_name: str = None):
    failed = total - passed
    return Row(
        table_name=table_name,
        check_name=check_name,
        column_name=column_name,
        total_rows=total,
        passed=passed,
        failed=failed,
        pass_rate=round(passed / total, 4) if total else 1.0,
    )

# Append rows to silver_quality_metrics after each check
```

### Logging output (console)

```
[silver DQ] silver_customers completeness (email): 9950/10000 passed (99.50%)
[silver DQ] silver_customers uniqueness (customer_id): 9990/10000 passed (99.90%)
[silver DQ] silver_orders referential_integrity (customer_id): 99950/100000 passed (99.95%)
[silver DQ] silver_orders overall PASS: 99530/100000 (99.53%)
```

---

## Alerting and Escalation

| Pass rate | Action |
|-----------|--------|
| ≥ 99.5% | Normal — log metrics |
| 99.0% – 99.5% | Warning — review `silver_*_quarantine` views |
| < 99.0% | Critical — halt Gold promotion; notify data owner |

For assessment sample data, expect ~**99.4%–99.9%** per check, consistent with ~700 injected defects across 110,500 rows.

---

## Related Documents

| Document | Purpose |
|----------|---------|
| `design-notes.md` | Silver layer design and required fields |
| `requirements-analysis.md` | Acceptance criteria for DQ |
| `.cursorrules` | Flag-don't-delete policy, logging, row counts |
| `src/silver/01–04_*.py` | Check implementations |
| `debugging-notes.md` | Root-cause notes for DQ surprises |
