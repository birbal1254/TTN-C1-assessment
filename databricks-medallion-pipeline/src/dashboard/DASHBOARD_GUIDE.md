# Databricks SQL Dashboard Guide

Step-by-step instructions to build an e-commerce sales dashboard on **Databricks Community Edition** using queries from `dashboard_queries.sql`.

---

## Prerequisites

Before creating the dashboard, run the pipeline on your Community Edition cluster:

1. Bronze: `%run ../bronze/ingest_all`
2. Silver: `%run ../silver/create_silver_tables`
3. Gold: `%run ../gold/create_gold_tables`

Confirm these tables exist in the SQL editor:

```sql
SHOW TABLES;
-- Expect: gold_sales_by_product, gold_revenue_by_customer, gold_customer_segmentation, silver_orders
```

**Community Edition notes:**

- One active cluster per account; start your cluster before opening the SQL warehouse or running queries.
- SQL warehouses on CE are often backed by the same all-purpose cluster—if queries fail with “warehouse not running,” start the cluster from **Compute**.
- Table names are typically `gold_sales_by_product` (no catalog prefix) unless you configured Unity Catalog.

---

## 1. Navigate to Databricks SQL Dashboard

1. Log in to [Databricks Community Edition](https://community.cloud.databricks.com/) (or your CE workspace URL).
2. In the left sidebar, click **SQL** (SQL editor icon).
3. If prompted, select or create a **SQL warehouse**:
   - CE: choose the default warehouse tied to your cluster, or use **SQL editor** with cluster attached.
   - Wait until status shows **Running** (green).
4. Open the **Dashboards** tab:
   - Top navigation: **Dashboards** or **Create** → **Dashboard**
   - In some CE layouts: left sidebar under SQL → **Dashboards** / **Lakeview**

**Screenshot description:** Left nav shows **Workspace**, **Compute**, **SQL**. Under SQL you see **SQL Editor**, **Queries**, **Dashboards**. The main area shows “Create dashboard” or a list of existing dashboards.

---

## 2. Create a New Dashboard

1. Click **Create dashboard** (or **+ New** → **Dashboard**).
2. Name the dashboard: `E-Commerce Sales Analytics`.
3. Optional description: `Medallion pipeline Gold layer — products, customers, revenue trends`.
4. Click **Create** / **Save**.

You land on an empty canvas with **Add** or **+** to add visualizations (tiles).

**Screenshot description:** Empty dashboard canvas with title “E-Commerce Sales Analytics”, a **+ Add visualization** button, and a blank grid layout.

---

## 3. Add Visualizations

For each tile: **Add visualization** → **Create from SQL** (or paste query in SQL editor, then **Add to dashboard**).

Source file: `src/dashboard/dashboard_queries.sql`

---

### Visualization 1: Top 10 Products by Revenue

**Title:** Top 10 Products by Revenue  
**Description:** Highest-revenue products from `gold_sales_by_product` (Silver PASS orders only).

**SQL** (copy Query 1 from `dashboard_queries.sql`):

```sql
SELECT
  product_name,
  category,
  total_revenue,
  total_quantity,
  total_orders
FROM gold_sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;
```

| Setting | Value |
|---------|--------|
| **Chart type** | Bar chart → **Horizontal bar** (if available, else vertical bar) |
| **X-axis** | `total_revenue` (numeric) |
| **Y-axis** | `product_name` (category) |
| **Color** | By `category` (optional — different color per product category) |
| **Sort** | Already sorted in SQL |

**Screenshot description:** Horizontal bars; product names on the left (Y), revenue on the bottom (X). Longest bar is the top product. Legend shows categories (Electronics, Clothing, etc.) if colored by `category`.

---

### Visualization 2: Customer Revenue Distribution

**Title:** Customer Revenue Distribution  
**Description:** Histogram of customers by lifetime revenue bucket from `gold_revenue_by_customer`.

**SQL** (Query 2):

```sql
SELECT
  revenue_bucket,
  customer_count
FROM (
  SELECT
    CASE
      WHEN total_revenue < 100 THEN '0-100'
      WHEN total_revenue < 500 THEN '100-500'
      WHEN total_revenue < 1000 THEN '500-1000'
      WHEN total_revenue < 5000 THEN '1000-5000'
      WHEN total_revenue < 10000 THEN '5000-10000'
      ELSE '10000+'
    END AS revenue_bucket,
    COUNT(*) AS customer_count
  FROM gold_revenue_by_customer
  GROUP BY 1
) AS revenue_distribution
ORDER BY
  CASE revenue_bucket
    WHEN '0-100' THEN 1
    WHEN '100-500' THEN 2
    WHEN '500-1000' THEN 3
    WHEN '1000-5000' THEN 4
    WHEN '5000-10000' THEN 5
    WHEN '10000+' THEN 6
    ELSE 7
  END;
```

| Setting | Value |
|---------|--------|
| **Chart type** | Bar chart (column) |
| **X-axis** | `revenue_bucket` |
| **Y-axis** | `customer_count` |
| **Color** | Single series color (e.g. blue) or gradient by bucket |

**Screenshot description:** Six bars from `0-100` through `10000+`. Most customers typically sit in lower buckets; `10000+` is a small bar on the right.

---

### Visualization 3: Customer Segmentation Breakdown

**Title:** Customer Segmentation Mix  
**Description:** Share of customers in each behavioral segment from `gold_customer_segmentation`.

**SQL** (Query 3):

```sql
SELECT
  segment_type,
  customer_count,
  pct_of_total_customers,
  total_revenue,
  avg_revenue,
  avg_orders
FROM gold_customer_segmentation
ORDER BY customer_count DESC;
```

| Setting | Value |
|---------|--------|
| **Chart type** | Pie chart (or Donut) |
| **Slice labels** | `segment_type` |
| **Slice values** | `customer_count` (alternative: `pct_of_total_customers` for % labels) |
| **Color** | Auto by segment; suggest: High-Value = green, Inactive = gray, One-Time = orange |

**Screenshot description:** Pie with slices labeled High-Value, Regular, Repeat, One-Time, Inactive. Largest slice is usually Regular or Repeat. Percent labels from `pct_of_total_customers` if enabled in chart settings.

---

### Visualization 4 (Bonus): Monthly Revenue Trend

**Title:** Monthly Revenue Trend  
**Description:** Sum of PASS order revenue by month from `silver_orders`.

**SQL** (Query 4):

```sql
SELECT
  DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
  SUM(CAST(total_amount AS DOUBLE)) AS monthly_revenue,
  COUNT(DISTINCT order_id) AS monthly_orders
FROM silver_orders
WHERE quality_check_result = 'PASS'
GROUP BY DATE_TRUNC('month', CAST(order_date AS DATE))
ORDER BY month;
```

| Setting | Value |
|---------|--------|
| **Chart type** | Line chart |
| **X-axis** | `month` (time / date) |
| **Y-axis** | `monthly_revenue` |
| **Secondary series** (optional) | `monthly_orders` on second Y-axis |
| **Color** | Single line color (e.g. `#1f77b4`) |

**Screenshot description:** Line chart from 2023–2024 with monthly points connected. X-axis shows months; Y-axis shows revenue. Upward or seasonal patterns visible across the sample data range.

---

## 4. Add Dashboard Filters

Filters apply across tiles that use compatible columns.

### Date range filter (Visualization 4)

1. On the dashboard, click **Add filter** (or **Parameters**).
2. Create filter:
   - **Name:** `order_date_range`
   - **Type:** Date range
   - **Column:** `order_date` on `silver_orders`
3. Update Query 4 SQL to use the filter (Databricks parameter syntax):

```sql
SELECT
  DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
  SUM(CAST(total_amount AS DOUBLE)) AS monthly_revenue,
  COUNT(DISTINCT order_id) AS monthly_orders
FROM silver_orders
WHERE quality_check_result = 'PASS'
  AND order_date BETWEEN :order_date_range.min AND :order_date_range.max
GROUP BY DATE_TRUNC('month', CAST(order_date AS DATE))
ORDER BY month;
```

**CE note:** If parameter widgets are not available on your CE build, hard-code a range in SQL:

```sql
AND order_date >= '2023-01-01' AND order_date <= '2024-12-31'
```

### Customer segment filter (Visualizations 2–3)

Segment filter applies to customer-level Gold tables.

1. **Add filter** → **Dropdown** or **Text**.
2. **Name:** `customer_segment`
3. For Query 2, wrap with filter on `gold_revenue_by_customer`:

```sql
-- Add to inner query WHERE clause:
FROM gold_revenue_by_customer
WHERE customer_segment IN (:customer_segment)
```

4. For Query 3, segments are already aggregated (`segment_type`). Use a **tile filter** instead:
   - Click Visualization 3 → **Filters** → **segment_type** → allow multi-select.

**Alternative:** Filter at dashboard level using **cross-filtering** (click a pie slice to filter other tiles)—enable in dashboard settings if supported.

**Screenshot description:** Top of dashboard shows filter bar with “Date range: 2023-01-01 to 2024-12-31” and dropdown “Customer segment: All / Premium / Standard / Basic”. Tiles refresh when filters change.

---

## 5. Arrange Tiles on the Dashboard

1. **Drag** each visualization tile to position it on the grid.
2. **Resize** using corner handles (CE grid is usually 12 columns wide).

**Recommended layout:**

```
┌─────────────────────────────┬─────────────────────────────┐
│  Viz 1: Top 10 Products     │  Viz 3: Segmentation Pie    │
│  (wide horizontal bar)      │                             │
├─────────────────────────────┴─────────────────────────────┤
│  Viz 4: Monthly Revenue Trend (full width line chart)     │
├─────────────────────────────┬─────────────────────────────┤
│  Viz 2: Revenue Histogram   │  (optional KPI / table)     │
└─────────────────────────────┴─────────────────────────────┘
```

3. Set each tile **Title** and **Description** in visualization settings (pencil icon).
4. Click **Save** / **Publish** to persist layout.

**Screenshot description:** Four tiles on a 2×2 grid with consistent padding. Title bar at top with filters. Line chart spans full width on row 2.

---

## 6. Screenshot Descriptions (Expected Results)

Use these as a checklist when validating your dashboard.

| Tile | What you should see |
|------|---------------------|
| **Top 10 Products** | Ten product names; revenue bars in descending order; top product often Electronics or high-price category from sample data. |
| **Revenue histogram** | Six buckets; highest `customer_count` in `0-100` or `100-500` buckets; small count in `10000+`. |
| **Segmentation pie** | Five segments: High-Value, Repeat, One-Time, Inactive, Regular; percentages sum to ~100%. |
| **Monthly trend** | Continuous line from Jan 2023 through Dec 2024; no NULL months unless data gap; values in thousands–millions depending on scale. |

**Empty tile troubleshooting (CE):**

- Cluster or SQL warehouse not running → start cluster, rerun pipeline.
- Table not found → run `create_gold_tables.py` on the same metastore.
- Zero rows → check Silver `quality_check_result = 'PASS'` counts.

---

## 7. Share the Dashboard

### Community Edition sharing options

1. **Within your account:** Dashboards are visible under **SQL → Dashboards** when logged in as the workspace owner (typical CE setup: single user).
2. **Share link (if enabled):**
   - Open dashboard → **Share** (top right).
   - Copy **link** for view-only access.
   - CE may restrict sharing to users on the same workspace only—no public internet links on free tier.
3. **Export / snapshot:**
   - **Download** chart as PNG (per tile menu) for slides or assessment submission.
   - Run queries in SQL editor → **Download full results** as CSV for offline sharing.

### Sharing checklist

| Action | CE limitation |
|--------|----------------|
| Email invite collaborators | Often not available on CE (single-user workspace) |
| Embed in external site | Not supported on CE |
| Scheduled refresh | Limited; manually refresh after pipeline runs |
| PNG export per chart | Usually available |

**For assessment submission:** Export PNGs of all four visualizations plus a screenshot of the full dashboard layout and note the date filters applied.

---

## Quick Reference

| Visualization | Table(s) | Chart | Key columns |
|---------------|----------|-------|-------------|
| Top products | `gold_sales_by_product` | Horizontal bar | `product_name`, `total_revenue` |
| Revenue histogram | `gold_revenue_by_customer` | Bar | `revenue_bucket`, `customer_count` |
| Segmentation | `gold_customer_segmentation` | Pie | `segment_type`, `customer_count` |
| Monthly trend | `silver_orders` | Line | `month`, `monthly_revenue` |

**Related files:**

- Queries: `src/dashboard/dashboard_queries.sql`
- Pipeline: `src/gold/create_gold_tables.py`, `src/silver/create_silver_tables.py`
