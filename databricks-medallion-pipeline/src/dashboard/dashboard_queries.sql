-- =============================================================================
-- Databricks SQL Dashboard Queries
-- E-commerce Medallion Pipeline — Gold / Silver analytics
-- =============================================================================
-- Prerequisites:
--   - gold_sales_by_product, gold_revenue_by_customer, gold_customer_segmentation
--   - silver_orders (for monthly trend)
-- Run create_gold_tables.py and create_silver_tables.py before using these queries.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- QUERY 1: Top 10 Products by Revenue
-- -----------------------------------------------------------------------------
-- Chart type:  Horizontal bar chart
-- X-axis:      total_revenue (numeric)
-- Y-axis:      product_name (category / label)
-- Optional filters:
--   - category = 'Electronics'  (focus one product line)
--   - total_revenue > 1000      (exclude low performers)
-- Sort:        total_revenue DESC (already in query)

SELECT
  product_name,
  category,
  total_revenue,
  total_quantity,
  total_orders
FROM gold_sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- QUERY 2: Customer Revenue Distribution (Histogram)
-- -----------------------------------------------------------------------------
-- Chart type:  Bar chart / histogram
-- X-axis:      revenue_bucket (ordered category)
-- Y-axis:      customer_count (numeric)
-- Optional filters:
--   - country = 'USA'           (geographic slice)
--   - customer_segment = 'Premium' (source segment from silver)
-- Note: Buckets built from gold_revenue_by_customer.total_revenue

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


-- -----------------------------------------------------------------------------
-- QUERY 3: Customer Segmentation Breakdown
-- -----------------------------------------------------------------------------
-- Chart type:  Pie chart (or donut)
-- Slice label: segment_type
-- Slice value: customer_count  (or pct_of_total_customers for % view)
-- Optional filters:
--   - segment_type IN ('High-Value', 'Repeat')  (focus active segments)
--   - customer_count > 10                       (hide tiny segments if needed)

SELECT
  segment_type,
  customer_count,
  pct_of_total_customers,
  total_revenue,
  avg_revenue,
  avg_orders
FROM gold_customer_segmentation
ORDER BY customer_count DESC;


-- -----------------------------------------------------------------------------
-- QUERY 4 (Bonus): Monthly Revenue Trend
-- -----------------------------------------------------------------------------
-- Chart type:  Line chart (time series)
-- X-axis:      month (date/timestamp)
-- Y-axis:      monthly_revenue (numeric)
-- Optional filters:
--   - order_date >= DATE_SUB(CURRENT_DATE(), 365)   (last 12 months only)
--   - order_status = 'Completed'                    (if column needed in Silver)
-- Source: PASS rows only from silver_orders

SELECT
  DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
  SUM(CAST(total_amount AS DOUBLE)) AS monthly_revenue,
  COUNT(DISTINCT order_id) AS monthly_orders
FROM silver_orders
WHERE quality_check_result = 'PASS'
GROUP BY DATE_TRUNC('month', CAST(order_date AS DATE))
ORDER BY month;
