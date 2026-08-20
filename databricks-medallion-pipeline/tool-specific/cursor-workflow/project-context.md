# Project Context

<!-- Placeholder: Context for Cursor AI when working on this project. -->

## Project

Databricks Medallion Architecture pipeline for e-commerce sales data.

## Domain

Customers, orders, products — sales analytics and customer segmentation.

## Layers

- Bronze: `src/bronze/`
- Silver: `src/silver/` (includes data quality)
- Gold: `src/gold/`
- Dashboard: `src/dashboard/`

## Key Docs

- `requirements-analysis.md`, `design-notes.md`, `data-model.md`
- `data-quality-strategy.md`

## Conventions

- Python for ingestion and orchestration; SQL for gold aggregations
- Delta Lake tables in bronze/silver/gold schemas
- Numbered scripts indicate execution order within each layer
