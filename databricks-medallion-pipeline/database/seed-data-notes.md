# Seed Data Notes

<!-- Placeholder: Notes on seed/sample data for development and testing. -->

## Source Files

Sample CSVs live in `data/` and are referenced by Bronze ingestion scripts.

## Seed Strategy

- Use `src/data_generation/generate_sample_data.py` to populate CSVs
- Alternatively, manually add rows following `data-model.md` constraints

## Referential Integrity

Ensure `orders.customer_id` and `orders.product_id` reference valid IDs in customers and products files.

## Volume Guidelines

| File | Suggested minimum rows |
|------|------------------------|
| customers.csv | 10+ |
| products.csv | 5+ |
| orders.csv | 20+ |
