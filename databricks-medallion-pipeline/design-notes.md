# Design Notes

<!-- Placeholder: Architecture and design decisions for the Medallion pipeline. -->

## Architecture Overview

```
CSV Sources → Bronze (raw Delta) → Silver (cleansed + DQ) → Gold (aggregates) → Dashboard
```

## Layer Responsibilities

### Bronze

- Land raw data with minimal transformation
- Preserve source schema and ingestion metadata (`ingested_at`, `source_file`)

### Silver

- Standardize types and naming
- Apply data quality rules (see `data-quality-strategy.md`)
- Enforce referential integrity between customers, orders, and products

### Gold

- Business-level aggregations and dimensions
- Optimized for BI and dashboard consumption

## Technology Choices

- **Storage**: Delta Lake
- **Compute**: Databricks Spark
- **Orchestration**: _Document job/workflow approach_

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Medallion layers | Clear separation of raw, cleansed, and analytical data |
| Separate DQ scripts | Modular, testable quality checks in Silver |

_Add additional design decisions as the project evolves._
