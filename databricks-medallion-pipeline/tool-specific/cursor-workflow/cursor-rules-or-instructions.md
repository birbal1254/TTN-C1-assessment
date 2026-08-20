# Cursor Rules or Instructions

<!-- Placeholder: Rules and instructions for Cursor when editing this repo. -->

## Code Style

- Follow existing naming: `01_`, `02_` prefixes for ordered scripts
- Use clear module docstrings describing layer and purpose
- Prefer Spark SQL or PySpark consistent with surrounding files

## Architecture

- Do not skip Medallion layers (no direct CSV → Gold)
- Data quality belongs in Silver, not Bronze or Gold
- Gold layer should only read from Silver

## Documentation

- Update `data-model.md` when schema changes
- Update `data-quality-strategy.md` when adding DQ rules

## Safety

- No hardcoded credentials or workspace URLs
- Use configuration parameters for catalog and paths

## Testing

- Validate with sample data in `data/` before production runs
