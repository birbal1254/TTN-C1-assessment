-- =============================================================================
-- E-Commerce Medallion Pipeline — Full Database Schema
-- Platform: Databricks SQL (Delta Lake)
-- Database: ecommerce_medallion
-- =============================================================================
-- Usage:
--   Run this script in Databricks SQL or a notebook with %sql
--   Pipeline PySpark jobs may create/overwrite these tables on ingest;
--   this DDL documents the canonical schema and supports manual setup.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ecommerce_medallion
COMMENT 'E-commerce sales data — Medallion Architecture (Bronze → Silver → Gold)';

USE ecommerce_medallion;


-- =============================================================================
-- BRONZE LAYER — Raw ingestion from daily CSV extracts
-- Purpose: Land source data with minimal transformation; preserve audit metadata
-- =============================================================================

-- Raw customer master from customers.csv (~10,000 rows/day)
CREATE TABLE IF NOT EXISTS bronze_customers (
  customer_id      BIGINT          COMMENT 'Primary key — unique customer identifier',
  customer_name    STRING          COMMENT 'Customer full name from source system',
  email            STRING          COMMENT 'Contact email address (may be NULL in source)',
  country          STRING          COMMENT 'Customer country',
  signup_date      DATE            COMMENT 'Account creation date',
  customer_segment STRING          COMMENT 'Marketing segment from source (Premium/Standard/Basic)',
  lifetime_value   DECIMAL(18, 2)  COMMENT 'Lifetime value from source CRM (may differ from computed Gold LTV)',
  ingestion_timestamp TIMESTAMP     COMMENT 'When the row was ingested into Bronze',
  source_file      STRING          COMMENT 'Path to source CSV file on S3/DBFS'
)
USING DELTA
COMMENT 'Bronze layer: raw customer records plus ingestion audit columns';

COMMENT ON TABLE bronze_customers IS
  'Bronze layer: daily customer CSV landing table. No business transformations applied.';


-- Raw order transactions from orders.csv (~100,000 rows/day)
CREATE TABLE IF NOT EXISTS bronze_orders (
  order_id      BIGINT          COMMENT 'Primary key — unique order identifier',
  customer_id   BIGINT          COMMENT 'Foreign key to bronze_customers.customer_id',
  order_date    DATE            COMMENT 'Date the order was placed',
  product_id    BIGINT          COMMENT 'Foreign key to bronze_products.product_id',
  quantity      INT             COMMENT 'Units ordered',
  unit_price    DECIMAL(18, 2)  COMMENT 'Price per unit at time of order',
  total_amount  DECIMAL(18, 2)  COMMENT 'Line total amount (quantity × unit_price)',
  order_status  STRING          COMMENT 'Order status: Completed, Pending, Cancelled, etc.',
  payment_date  DATE            COMMENT 'Payment cleared date (NULL if unpaid/cancelled)',
  ingestion_timestamp TIMESTAMP COMMENT 'When the row was ingested into Bronze',
  source_file      STRING       COMMENT 'Path to source CSV file on S3/DBFS'
)
USING DELTA
COMMENT 'Bronze layer: raw order records plus ingestion audit columns';

COMMENT ON TABLE bronze_orders IS
  'Bronze layer: daily order CSV landing table. Referential integrity validated in Silver.';


-- Raw product catalog from products.csv (~500 rows/day)
CREATE TABLE IF NOT EXISTS bronze_products (
  product_id     BIGINT          COMMENT 'Primary key — unique product identifier',
  product_name   STRING          COMMENT 'Product display name',
  category       STRING          COMMENT 'Product category (Electronics, Clothing, etc.)',
  price          DECIMAL(18, 2)  COMMENT 'Current list price',
  cost           DECIMAL(18, 2)  COMMENT 'Unit cost for margin analysis',
  stock_quantity INT             COMMENT 'Units in stock',
  reorder_level  INT             COMMENT 'Minimum stock before reorder alert',
  ingestion_timestamp TIMESTAMP  COMMENT 'When the row was ingested into Bronze',
  source_file      STRING       COMMENT 'Path to source CSV file on S3/DBFS'
)
USING DELTA
COMMENT 'Bronze layer: raw product catalog plus ingestion audit columns';

COMMENT ON TABLE bronze_products IS
  'Bronze layer: daily product CSV landing table. Clean catalog with no intentional DQ defects.';


-- =============================================================================
-- SILVER LAYER — Cleansed, validated data with quality flags
-- Purpose: Apply DQ checks; flag bad rows (never delete); retain full row counts
-- =============================================================================

-- Cleansed customers with combined quality_check_result from all Silver DQ rules
CREATE TABLE IF NOT EXISTS silver_customers (
  customer_id           BIGINT          COMMENT 'Primary key — unique customer identifier',
  customer_name         STRING          COMMENT 'Customer full name',
  email                 STRING          COMMENT 'Contact email address',
  country               STRING          COMMENT 'Customer country',
  signup_date           DATE            COMMENT 'Account creation date',
  customer_segment      STRING          COMMENT 'Marketing segment from source',
  lifetime_value        DECIMAL(18, 2)  COMMENT 'Lifetime value from source CRM',
  ingestion_timestamp   TIMESTAMP       COMMENT 'Bronze ingestion timestamp (carried forward)',
  source_file           STRING          COMMENT 'Bronze source file path (carried forward)',
  quality_check_result  STRING          COMMENT 'PASS or FAIL: {reasons} — combined DQ outcome'
)
USING DELTA
COMMENT 'Silver layer: validated customers; all Bronze rows retained with quality flag';

COMMENT ON TABLE silver_customers IS
  'Silver layer: customer records after completeness, uniqueness, and type validation checks.';


-- Cleansed orders with combined quality_check_result
CREATE TABLE IF NOT EXISTS silver_orders (
  order_id              BIGINT          COMMENT 'Primary key — unique order identifier',
  customer_id           BIGINT          COMMENT 'Foreign key to silver_customers.customer_id',
  order_date            DATE            COMMENT 'Date the order was placed',
  product_id            BIGINT          COMMENT 'Foreign key to silver_products.product_id',
  quantity              INT             COMMENT 'Units ordered',
  unit_price            DECIMAL(18, 2)  COMMENT 'Price per unit at time of order',
  total_amount          DECIMAL(18, 2)  COMMENT 'Line total amount',
  order_status          STRING          COMMENT 'Order status',
  payment_date          DATE            COMMENT 'Payment cleared date',
  ingestion_timestamp   TIMESTAMP       COMMENT 'Bronze ingestion timestamp (carried forward)',
  source_file           STRING          COMMENT 'Bronze source file path (carried forward)',
  quality_check_result  STRING          COMMENT 'PASS or FAIL: {reasons} — combined DQ outcome'
)
USING DELTA
COMMENT 'Silver layer: validated orders; orphan FKs flagged not deleted';

COMMENT ON TABLE silver_orders IS
  'Silver layer: order records after completeness, uniqueness, type, and referential integrity checks.';


-- Cleansed products with combined quality_check_result
CREATE TABLE IF NOT EXISTS silver_products (
  product_id            BIGINT          COMMENT 'Primary key — unique product identifier',
  product_name          STRING          COMMENT 'Product display name',
  category              STRING          COMMENT 'Product category',
  price                 DECIMAL(18, 2)  COMMENT 'List price',
  cost                  DECIMAL(18, 2)  COMMENT 'Unit cost',
  stock_quantity        INT             COMMENT 'Units in stock',
  reorder_level         INT             COMMENT 'Reorder threshold',
  ingestion_timestamp   TIMESTAMP       COMMENT 'Bronze ingestion timestamp (carried forward)',
  source_file           STRING          COMMENT 'Bronze source file path (carried forward)',
  quality_check_result  STRING          COMMENT 'PASS or FAIL: {reasons} — combined DQ outcome'
)
USING DELTA
COMMENT 'Silver layer: validated product catalog';

COMMENT ON TABLE silver_products IS
  'Silver layer: product records after uniqueness and type validation checks.';


-- Data quality metrics emitted by create_silver_tables.py after each pipeline run
CREATE TABLE IF NOT EXISTS silver_quality_report (
  table_name  STRING         COMMENT 'Silver or Bronze table evaluated (e.g. silver_orders)',
  check_type  STRING         COMMENT 'DQ dimension: completeness, uniqueness, type_validation, referential_integrity, overall',
  total_rows  BIGINT         COMMENT 'Total rows evaluated for this check',
  passed      BIGINT         COMMENT 'Rows passing the check',
  failed      BIGINT         COMMENT 'Rows failing the check',
  pass_rate   DOUBLE         COMMENT 'Pass rate as decimal 0–1 (passed / total_rows)'
)
USING DELTA
COMMENT 'Silver layer: per-check data quality metrics from pipeline runs';

COMMENT ON TABLE silver_quality_report IS
  'Silver layer: aggregated DQ metrics report written by create_silver_tables.py orchestrator.';


-- =============================================================================
-- GOLD LAYER — Business aggregations for analytics and dashboards
-- Purpose: Trusted metrics built only from Silver rows where quality_check_result = PASS
-- =============================================================================

-- Product performance: revenue, quantity, and margin by product
CREATE TABLE IF NOT EXISTS gold_sales_by_product (
  product_id       BIGINT          COMMENT 'Product identifier',
  product_name     STRING          COMMENT 'Product display name',
  category         STRING          COMMENT 'Product category',
  total_orders     BIGINT          COMMENT 'Distinct order count per product (PASS orders only)',
  total_quantity   BIGINT          COMMENT 'Sum of quantity sold',
  total_revenue    DECIMAL(18, 2)  COMMENT 'Sum of order line revenue',
  avg_order_value  DECIMAL(18, 2)  COMMENT 'Average order line amount',
  profit_margin    DECIMAL(8, 2)   COMMENT 'Catalog margin %: ((price - cost) / price) × 100'
)
USING DELTA
COMMENT 'Gold layer: sales aggregated by product for dashboard and BI';

COMMENT ON TABLE gold_sales_by_product IS
  'Gold layer: product sales metrics from PASS silver_orders joined to PASS silver_products.';


-- Customer value: revenue, order history, and recency per customer
CREATE TABLE IF NOT EXISTS gold_revenue_by_customer (
  customer_id            BIGINT          COMMENT 'Customer identifier',
  customer_name          STRING          COMMENT 'Customer full name',
  customer_segment       STRING          COMMENT 'Source marketing segment',
  country                STRING          COMMENT 'Customer country',
  total_orders           BIGINT          COMMENT 'Distinct order count per customer',
  total_revenue          DECIMAL(18, 2)  COMMENT 'Sum of order revenue (PASS orders only)',
  avg_order_value        DECIMAL(18, 2)  COMMENT 'Average order line amount',
  first_order_date       DATE            COMMENT 'Earliest PASS order date',
  last_order_date        DATE            COMMENT 'Most recent PASS order date',
  lifetime_value_actual  DECIMAL(18, 2)  COMMENT 'Computed LTV from order history (= total_revenue)',
  days_since_last_order  INT             COMMENT 'Days from today to last_order_date'
)
USING DELTA
COMMENT 'Gold layer: customer revenue and recency metrics';

COMMENT ON TABLE gold_revenue_by_customer IS
  'Gold layer: per-customer revenue metrics from PASS silver_orders joined to PASS silver_customers.';


-- Behavioral segment summary: distribution of customers across segments
CREATE TABLE IF NOT EXISTS gold_customer_segmentation (
  segment_type             STRING          COMMENT 'High-Value, Repeat, One-Time, Inactive, or Regular',
  customer_count           BIGINT          COMMENT 'Number of customers in this segment',
  avg_revenue              DECIMAL(18, 2)  COMMENT 'Average total_revenue per customer in segment',
  total_revenue            DECIMAL(18, 2)  COMMENT 'Sum of revenue across customers in segment',
  avg_orders               DOUBLE          COMMENT 'Average order count per customer in segment',
  pct_of_total_customers   DECIMAL(8, 2)   COMMENT 'Segment share of customer base (percentage)'
)
USING DELTA
COMMENT 'Gold layer: aggregated customer behavioral segmentation summary';

COMMENT ON TABLE gold_customer_segmentation IS
  'Gold layer: segment-level counts and revenue from classified gold_revenue_by_customer rows.';


-- =============================================================================
-- Optional: verify schema objects
-- =============================================================================
-- SHOW TABLES IN ecommerce_medallion;
-- DESCRIBE TABLE EXTENDED ecommerce_medallion.gold_sales_by_product;
