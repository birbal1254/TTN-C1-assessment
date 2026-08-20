# Requirements Analysis

<!-- Placeholder: E-commerce sales data pipeline requirements. -->

## Business Context

Process e-commerce sales data (customers, orders, products) through a Medallion Architecture on Databricks to support sales analytics, customer segmentation, and revenue reporting.

## Functional Requirements

1. Ingest customer, order, and product data from CSV sources (Bronze).
2. Validate completeness, uniqueness, types, referential integrity, and business rules (Silver).
3. Produce gold-layer metrics: sales by product, revenue by customer, trends, segmentation.
4. Provide dashboard-ready SQL and documentation.

## Non-Functional Requirements

- Scalable batch processing on Databricks
- Delta Lake for ACID and time travel
- Traceable data quality failures
- Clear documentation for operators and analysts

## Data Sources

| Source | Description |
|--------|-------------|
| `customers.csv` | Customer master data |
| `orders.csv` | Order transactions |
| `products.csv` | Product catalog |

## Out of Scope

_List explicit exclusions here._
