# Database Setup Notes

<!-- Placeholder: How to configure catalog, schemas, and tables on Databricks. -->

## Prerequisites

- Databricks workspace with Unity Catalog or Hive metastore
- Cluster or SQL warehouse with Delta Lake support

## Setup Steps

1. Create catalog and schemas: `bronze`, `silver`, `gold`
2. Run `database/schema.sql` (or let pipeline scripts create tables on first write)
3. Upload or mount `data/*.csv` to accessible storage
4. Configure job parameters (paths, catalog names)

## Configuration

| Parameter | Example | Description |
|-----------|---------|-------------|
| `catalog` | `ecommerce` | Unity Catalog name |
| `bronze_schema` | `bronze` | Raw layer schema |
| `silver_schema` | `silver` | Cleansed layer schema |
| `gold_schema` | `gold` | Analytics layer schema |

## Troubleshooting

_Document common setup issues and fixes here._
