# Specification

<!-- Placeholder: Technical specification for the Medallion pipeline. -->

## Scope

End-to-end batch pipeline from CSV sources to gold analytics tables and dashboard queries.

## Inputs

- `data/customers.csv`, `data/orders.csv`, `data/products.csv`

## Outputs

- Bronze Delta tables (raw)
- Silver Delta tables (cleansed, DQ-validated)
- Gold tables: sales by product, revenue by customer, trends, segmentation
- Dashboard SQL and guide

## Acceptance Criteria

1. All three source files ingest successfully to Bronze
2. Silver quality checks run with logged pass/fail metrics
3. Gold tables match defined aggregations in `data-model.md`
4. Dashboard queries execute without errors against gold tables

## Dependencies

- Databricks Runtime with Delta Lake
- Python 3.x / PySpark
