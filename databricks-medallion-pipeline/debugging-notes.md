# Debugging Notes

<!-- Placeholder: Common issues and debugging steps for the pipeline. -->

## Bronze Layer

- **CSV not found**: Verify path to `data/*.csv` and mount configuration
- **Schema mismatch**: Compare CSV headers with expected schema in ingestion scripts

## Silver Layer

- **DQ failures**: Check quarantine tables and logs from quality scripts (01–05)
- **Empty silver tables**: Confirm bronze data exists and DQ thresholds are not too strict

## Gold Layer

- **SQL errors**: Validate silver table names and column references in gold SQL files
- **Unexpected aggregates**: Compare silver row counts vs. gold totals

## General

- Review Spark UI for stage failures and skew
- Use Delta table history for rollback investigation

_Log specific bugs and resolutions below._
