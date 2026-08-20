# Data Generation Notes

<!-- Placeholder: Notes on how sample data is generated and validated. -->

## Purpose

Generate realistic e-commerce sample data for local development and pipeline testing.

## Output Files

- `data/customers.csv`
- `data/orders.csv`
- `data/products.csv`

## Generation Rules

- Customers: unique IDs, valid email format, varied countries
- Products: unique SKUs, positive prices, product categories
- Orders: valid FK references, positive quantities, coherent dates and amounts

## Usage

```bash
python src/data_generation/generate_sample_data.py
```

## Notes

_Document seed ranges, row counts, and any randomization approach here._
