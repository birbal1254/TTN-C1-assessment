# Databricks Medallion Architecture Pipeline

A Databricks-based data pipeline for e-commerce sales data, organized using the Medallion Architecture (Bronze, Silver, Gold). This project ingests raw customer, order, and product data, applies data quality checks in the Silver layer, and produces business-ready analytics tables and dashboard queries in the Gold layer.

## Project Structure

| Layer | Purpose |
|-------|---------|
| **Bronze** | Raw ingestion of source CSV files into Delta tables |
| **Silver** | Cleansed, validated, and conformed data with quality checks |
| **Gold** | Aggregated business metrics and analytical views |
| **Dashboard** | SQL queries and guides for reporting |

## Getting Started

1. Review `requirements-analysis.md` and `design-notes.md` for project context.
2. Generate sample data with `src/data_generation/generate_sample_data.py`.
3. Run Bronze ingestion scripts, then Silver quality checks, then Gold table creation.
4. Use `src/dashboard/dashboard_queries.sql` for reporting.

## Documentation

See `tool-workflow.md`, `data-model.md`, and `data-quality-strategy.md` for detailed guidance.
