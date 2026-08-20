# Data Model

<!-- Placeholder: Entity relationships and table definitions for e-commerce sales data. -->

## Entities

### Customer

- `customer_id` (PK)
- `name`, `email`, `country`, `created_at`

### Product

- `product_id` (PK)
- `name`, `category`, `price`, `sku`

### Order

- `order_id` (PK)
- `customer_id` (FK → Customer)
- `product_id` (FK → Product)
- `quantity`, `order_date`, `status`, `total_amount`

## Relationships

```
Customer 1───* Order
Product  1───* Order
```

## Layer Mapping

| Bronze | Silver | Gold |
|--------|--------|------|
| `bronze.customers` | `silver.customers` | `gold.revenue_by_customer` |
| `bronze.orders` | `silver.orders` | `gold.sales_by_product` |
| `bronze.products` | `silver.products` | `gold.daily_weekly_trends` |

See `database/schema.sql` for DDL placeholders.
