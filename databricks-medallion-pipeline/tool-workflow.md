# Tool Workflow

<!-- Placeholder: Document the tools and workflow used to build this pipeline. -->

## Tools

- **Databricks** — Notebook/job execution, Delta Lake storage
- **Cursor** — AI-assisted development (see `tool-specific/cursor-workflow/`)
- **Git** — Version control and collaboration

## Workflow

1. **Plan** — Requirements and design (`requirements-analysis.md`, `design-notes.md`)
2. **Generate data** — `src/data_generation/`
3. **Bronze** — Ingest raw CSVs → Delta bronze tables
4. **Silver** — Quality checks → cleansed silver tables
5. **Gold** — Aggregations and business metrics
6. **Dashboard** — Reporting queries and guides
7. **Debug & reflect** — `debugging-notes.md`, `reflection.md`

## Environment Setup

_Document cluster, catalog, and schema configuration here._
