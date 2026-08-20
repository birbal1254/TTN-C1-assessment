# Data Quality Strategy

<!-- Placeholder: Data quality rules and implementation approach for Silver layer. -->

## Quality Dimensions

| Dimension | Silver Script | Description |
|-----------|---------------|-------------|
| Completeness | `01_quality_completeness.py` | Required fields non-null |
| Uniqueness | `02_quality_uniqueness.py` | Primary keys unique |
| Type validation | `03_quality_type_validation.py` | Correct data types and formats |
| Referential integrity | `04_quality_referential_integrity.py` | FK relationships valid |
| Business logic | `05_quality_business_logic.py` | Domain rules (e.g. positive quantities, valid dates) |

## Failure Handling

- Log failed records to quarantine or DQ audit tables
- Do not promote invalid records to downstream Silver/Gold tables
- Document thresholds and escalation in operations runbooks

## Metrics

- Record counts by layer
- DQ pass/fail rates per rule
- Quarantine volume over time

## Implementation Notes

_Quality checks run as part of `create_silver_tables.py` orchestration._
