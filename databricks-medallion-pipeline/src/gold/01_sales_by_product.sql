-- Gold layer: sales aggregated by product
-- Business question: Which products drive order volume and revenue?
--
-- Logic:
--   1. Use only quality-passed Silver rows (trusted data).
--   2. Join orders to products on product_id.
--   3. Aggregate counts and sums per product.
--   4. Compute profit margin from catalog price and cost.
--
-- Source: silver_orders (PASS) JOIN silver_products (PASS)
-- Target: gold_sales_by_product

CREATE OR REPLACE TABLE gold_sales_by_product AS
WITH valid_orders AS (
  -- Order lines that passed all Silver data quality checks
  SELECT
    o.order_id,
    o.product_id,
    o.quantity,
    o.total_amount
  FROM silver_orders AS o
  WHERE o.quality_check_result = 'PASS'
),
valid_products AS (
  -- Product catalog rows that passed all Silver data quality checks
  SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.cost
  FROM silver_products AS p
  WHERE p.quality_check_result = 'PASS'
),
product_sales AS (
  SELECT
    p.product_id,
    p.product_name,
    p.category,
    -- Distinct order count per product (not just order-line count)
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.quantity) AS total_quantity,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    -- Margin % from catalog price/cost: ((price - cost) / price) * 100
    ROUND(
      ((MAX(p.price) - MAX(p.cost)) / MAX(p.price)) * 100,
      2
    ) AS profit_margin
  FROM valid_orders AS o
  INNER JOIN valid_products AS p
    ON o.product_id = p.product_id
  GROUP BY
    p.product_id,
    p.product_name,
    p.category
)
SELECT
  product_id,
  product_name,
  category,
  total_orders,
  total_quantity,
  total_revenue,
  avg_order_value,
  profit_margin
FROM product_sales
ORDER BY total_revenue DESC;
