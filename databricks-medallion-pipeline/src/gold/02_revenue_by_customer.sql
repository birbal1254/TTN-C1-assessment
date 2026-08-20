-- Gold layer: revenue aggregated by customer
-- Business question: Who are our highest-value customers and how recently did they purchase?
--
-- Logic:
--   1. Use only quality-passed Silver rows on both orders and customers.
--   2. Join orders to customers on customer_id.
--   3. Aggregate order metrics per customer.
--   4. Derive lifetime_value_actual from total revenue and recency from last order date.
--
-- Source: silver_orders (PASS) JOIN silver_customers (PASS)
-- Target: gold_revenue_by_customer

CREATE OR REPLACE TABLE gold_revenue_by_customer AS
WITH valid_orders AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount
  FROM silver_orders AS o
  WHERE o.quality_check_result = 'PASS'
),
valid_customers AS (
  SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.country
  FROM silver_customers AS c
  WHERE c.quality_check_result = 'PASS'
),
customer_revenue AS (
  SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.country,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
  FROM valid_orders AS o
  INNER JOIN valid_customers AS c
    ON o.customer_id = c.customer_id
  GROUP BY
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.country
)
SELECT
  customer_id,
  customer_name,
  customer_segment,
  country,
  total_orders,
  total_revenue,
  avg_order_value,
  first_order_date,
  last_order_date,
  -- Actual lifetime value derived from order history (same as total_revenue here)
  total_revenue AS lifetime_value_actual,
  -- Days since the customer's most recent quality-passed order
  DATEDIFF(CURRENT_DATE(), last_order_date) AS days_since_last_order
FROM customer_revenue
ORDER BY total_revenue DESC;
