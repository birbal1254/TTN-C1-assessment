-- Gold layer: customer segmentation summary
-- Business question: How are customers distributed across behavioral segments?
--
-- Logic:
--   1. Start from per-customer metrics in gold_revenue_by_customer.
--   2. Classify each customer into one segment (priority order below).
--   3. Aggregate segment-level counts, revenue, and percentages.
--
-- Segment rules (first match wins):
--   - Inactive:   no order in 180+ days (days_since_last_order > 180)
--   - High-Value: total_revenue > 10000 OR total_orders > 20
--   - One-Time:   total_orders = 1
--   - Repeat:     total_orders BETWEEN 5 AND 20
--   - Regular:    all remaining customers
--
-- Source: gold_revenue_by_customer
-- Target: gold_customer_segmentation

CREATE OR REPLACE TABLE gold_customer_segmentation AS
WITH customer_metrics AS (
  SELECT
    customer_id,
    total_orders,
    total_revenue,
    last_order_date,
    days_since_last_order
  FROM gold_revenue_by_customer
),
classified_customers AS (
  SELECT
    customer_id,
    total_orders,
    total_revenue,
    CASE
      WHEN days_since_last_order > 180 THEN 'Inactive'
      WHEN total_revenue > 10000 OR total_orders > 20 THEN 'High-Value'
      WHEN total_orders = 1 THEN 'One-Time'
      WHEN total_orders >= 5 AND total_orders <= 20 THEN 'Repeat'
      ELSE 'Regular'
    END AS segment_type
  FROM customer_metrics
),
total_customer_base AS (
  SELECT COUNT(*) AS total_customers
  FROM classified_customers
)
SELECT
  cc.segment_type,
  COUNT(*) AS customer_count,
  AVG(cc.total_revenue) AS avg_revenue,
  SUM(cc.total_revenue) AS total_revenue,
  AVG(cc.total_orders) AS avg_orders,
  ROUND(COUNT(*) * 100.0 / MAX(tcb.total_customers), 2) AS pct_of_total_customers
FROM classified_customers AS cc
CROSS JOIN total_customer_base AS tcb
GROUP BY cc.segment_type
ORDER BY customer_count DESC;
