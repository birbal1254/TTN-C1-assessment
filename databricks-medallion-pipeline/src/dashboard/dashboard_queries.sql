-- Dashboard queries for e-commerce sales analytics
-- Placeholder: Ready-to-use queries for BI tools and dashboards

-- Top products by revenue
-- SELECT product_name, category, total_revenue
-- FROM gold.sales_by_product
-- ORDER BY total_revenue DESC
-- LIMIT 10;

-- Top customers by revenue
-- SELECT customer_name, country, total_revenue
-- FROM gold.revenue_by_customer
-- ORDER BY total_revenue DESC
-- LIMIT 10;

-- Daily revenue trend (last 30 days)
-- SELECT order_day, daily_revenue
-- FROM gold.daily_weekly_trends
-- ORDER BY order_day DESC
-- LIMIT 30;

-- Customer segmentation distribution
-- SELECT revenue_segment, COUNT(*) AS customer_count
-- FROM gold.customer_segmentation
-- GROUP BY revenue_segment;
