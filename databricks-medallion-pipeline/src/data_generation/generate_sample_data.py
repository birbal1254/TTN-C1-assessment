"""
Generate realistic e-commerce sample CSVs for the Databricks Medallion pipeline.

Creates customers.csv (10,000 rows), products.csv (500 rows), and orders.csv
(100,000 rows) with intentional data quality defects for Silver-layer testing.

Output: data/customers.csv, data/products.csv, data/orders.csv
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from faker import Faker

# Reproducible output for pipeline testing and assessment demos
RANDOM_SEED = 42

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

COUNTRIES = ["USA", "UK", "Canada", "Germany", "France", "Australia", "India"]
CUSTOMER_SEGMENTS = ["Premium", "Standard", "Basic"]
CUSTOMER_SEGMENT_WEIGHTS = [0.20, 0.50, 0.30]

PRODUCT_CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home",
    "Sports",
    "Books",
    "Beauty",
    "Food",
    "Toys",
]

ORDER_STATUSES = ["Completed", "Pending", "Cancelled"]
ORDER_STATUS_WEIGHTS = [0.70, 0.20, 0.10]

EMAIL_DOMAINS = ["example.com", "shopmail.com", "ecommerce.io", "customer.net"]

# Intentional defect counts (see data-quality-strategy.md)
NULL_EMAIL_COUNT = 50
DUPLICATE_CUSTOMER_ID_ROWS = 10
NULL_ORDER_CUSTOMER_ID_COUNT = 100
NULL_ORDER_PRODUCT_ID_COUNT = 200
ORPHAN_CUSTOMER_ID_COUNT = 50
ORPHAN_PRODUCT_ID_COUNT = 30
DUPLICATE_ORDER_ID_ROWS = 10  # 10 duplicated IDs → 20 rows involved in uniqueness failure


def money(value: float) -> str:
    """Format a numeric value as a two-decimal string."""
    return str(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def random_date(start: date, end: date) -> date:
    """Return a uniform random date in the inclusive range [start, end]."""
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def weighted_choice(options: list[str], weights: list[float]) -> str:
    """Return one option using the provided probability weights."""
    return random.choices(options, weights=weights, k=1)[0]


def lifetime_value_for_segment(segment: str) -> str:
    """Return lifetime_value correlated with customer segment."""
    ranges = {
        "Premium": (5000, 50000),
        "Standard": (1000, 5000),
        "Basic": (100, 1000),
    }
    low, high = ranges[segment]
    return money(random.uniform(low, high))


def build_email(first_name: str, last_name: str) -> str:
    """Build firstname.lastname@domain.com from name parts."""
    first = first_name.lower().replace(" ", "")
    last = last_name.lower().replace(" ", "")
    domain = random.choice(EMAIL_DOMAINS)
    return f"{first}.{last}@{domain}"


def generate_products(fake: Faker) -> list[dict[str, Any]]:
    """Generate 500 product catalog rows (clean — no intentional defects)."""
    products: list[dict[str, Any]] = []
    for product_id in range(1, 501):
        price = round(random.uniform(9.99, 999.99), 2)
        cost = round(price * random.uniform(0.3, 0.7), 2)
        products.append(
            {
                "product_id": product_id,
                "product_name": fake.catch_phrase().title()[:80],
                "category": random.choice(PRODUCT_CATEGORIES),
                "price": money(price),
                "cost": money(cost),
                "stock_quantity": random.randint(0, 1000),
                "reorder_level": random.randint(10, 100),
            }
        )
    return products


def generate_customers(fake: Faker) -> list[dict[str, Any]]:
    """Generate 10,000 customer rows with unique sequential customer_id values."""
    customers: list[dict[str, Any]] = []
    signup_start = date(2020, 1, 1)
    signup_end = date(2024, 12, 31)

    for customer_id in range(1, 10_001):
        first_name = fake.first_name()
        last_name = fake.last_name()
        segment = weighted_choice(CUSTOMER_SEGMENTS, CUSTOMER_SEGMENT_WEIGHTS)
        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": f"{first_name} {last_name}",
                "email": build_email(first_name, last_name),
                "country": random.choice(COUNTRIES),
                "signup_date": random_date(signup_start, signup_end).isoformat(),
                "customer_segment": segment,
                "lifetime_value": lifetime_value_for_segment(segment),
            }
        )
    return customers


def apply_customer_quality_issues(customers: list[dict[str, Any]]) -> dict[str, int]:
    """
    Inject intentional customer data quality defects.

    - 50 rows with NULL email (completeness failure)
    - 10 rows with duplicate customer_id (uniqueness failure — 10 IDs appear twice)
    """
    issues = {"null_email": 0, "duplicate_customer_id": 0}

    # COMPLETENESS: 50 rows with NULL email
    null_email_indices = random.sample(range(len(customers)), NULL_EMAIL_COUNT)
    for idx in null_email_indices:
        customers[idx]["email"] = None
        issues["null_email"] += 1

    # UNIQUENESS: pick 10 source rows and 10 target rows; copy source customer_id onto target
    # Result: 10 customer_id values each appear on 2 rows → 20 rows fail uniqueness check
    candidate_targets = [i for i in range(len(customers)) if i not in null_email_indices]
    target_indices = random.sample(candidate_targets, DUPLICATE_CUSTOMER_ID_ROWS)
    source_indices = random.sample(
        [i for i in range(len(customers)) if i not in target_indices],
        DUPLICATE_CUSTOMER_ID_ROWS,
    )
    for target_idx, source_idx in zip(target_indices, source_indices):
        customers[target_idx]["customer_id"] = customers[source_idx]["customer_id"]
        issues["duplicate_customer_id"] += 1

    return issues


def generate_orders(
    fake: Faker,
    product_price_lookup: dict[int, str],
) -> list[dict[str, Any]]:
    """Generate 100,000 order rows with valid FKs and derived amounts."""
    orders: list[dict[str, Any]] = []
    order_start = date(2023, 1, 1)
    order_end = date(2024, 12, 31)

    for order_id in range(1, 100_001):
        customer_id = random.randint(1, 10_000)
        product_id = random.randint(1, 500)
        quantity = random.randint(1, 10)
        unit_price = Decimal(product_price_lookup[product_id])
        total_amount = money(float(unit_price * quantity))
        order_date = random_date(order_start, order_end)
        order_status = weighted_choice(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)

        if order_status == "Completed":
            payment_date = order_date + timedelta(days=random.randint(0, 7))
        else:
            payment_date = None  # Pending/Cancelled → NULL payment_date

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.isoformat(),
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": money(float(unit_price)),
                "total_amount": total_amount,
                "order_status": order_status,
                "payment_date": payment_date.isoformat() if payment_date else None,
            }
        )
    return orders


def apply_order_quality_issues(orders: list[dict[str, Any]]) -> dict[str, int]:
    """
    Inject intentional order data quality defects.

    - 100 rows: NULL customer_id (completeness)
    - 200 rows: NULL product_id (completeness)
    - 50 rows: orphan customer_id 99901–99950 (referential integrity)
    - 30 rows: orphan product_id 9901–9930 (referential integrity)
    - 10 duplicated order_id values → 20 rows fail uniqueness (10 pairs)
    """
    issues = {
        "null_customer_id": 0,
        "null_product_id": 0,
        "orphan_customer_id": 0,
        "orphan_product_id": 0,
        "duplicate_order_id": 0,
    }

    used_indices: set[int] = set()

    def take_indices(count: int) -> list[int]:
        available = [i for i in range(len(orders)) if i not in used_indices]
        chosen = random.sample(available, count)
        used_indices.update(chosen)
        return chosen

    # COMPLETENESS: NULL customer_id on 100 rows
    for idx in take_indices(NULL_ORDER_CUSTOMER_ID_COUNT):
        orders[idx]["customer_id"] = None
        issues["null_customer_id"] += 1

    # COMPLETENESS: NULL product_id on 200 rows (disjoint index sets)
    for idx in take_indices(NULL_ORDER_PRODUCT_ID_COUNT):
        orders[idx]["product_id"] = None
        issues["null_product_id"] += 1

    # REFERENTIAL INTEGRITY: customer_id not in customers table (99901–99950)
    orphan_customer_ids = list(range(99_901, 99_951))
    for idx, orphan_id in zip(take_indices(ORPHAN_CUSTOMER_ID_COUNT), orphan_customer_ids):
        orders[idx]["customer_id"] = orphan_id
        issues["orphan_customer_id"] += 1

    # REFERENTIAL INTEGRITY: product_id not in products table (9901–9930)
    orphan_product_ids = list(range(9_901, 9_931))
    for idx, orphan_id in zip(take_indices(ORPHAN_PRODUCT_ID_COUNT), orphan_product_ids):
        orders[idx]["product_id"] = orphan_id
        issues["orphan_product_id"] += 1

    # UNIQUENESS: duplicate 10 order_id values across 10 target rows
    candidate_targets = [i for i in range(len(orders)) if i not in used_indices]
    target_indices = random.sample(candidate_targets, DUPLICATE_ORDER_ID_ROWS)
    source_indices = random.sample(
        [i for i in range(len(orders)) if i not in target_indices],
        DUPLICATE_ORDER_ID_ROWS,
    )
    for target_idx, source_idx in zip(target_indices, source_indices):
        orders[target_idx]["order_id"] = orders[source_idx]["order_id"]
        issues["duplicate_order_id"] += 1

    return issues


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    """Write rows to CSV, encoding None as an empty field (NULL for downstream reads)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: ("" if row[key] is None else row[key]) for key in fieldnames})


def print_summary(
    products: list[dict[str, Any]],
    customers: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    customer_issues: dict[str, int],
    order_issues: dict[str, int],
) -> None:
    """Print row counts and intentional quality issue summary."""
    total_rows = len(customers) + len(orders) + len(products)
    total_issues = sum(customer_issues.values()) + sum(order_issues.values())

    print("=" * 60)
    print("Sample Data Generation Summary")
    print("=" * 60)
    print(f"Output directory: {DATA_DIR}")
    print()
    print("Row counts:")
    print(f"  customers.csv : {len(customers):>7,} rows")
    print(f"  products.csv  : {len(products):>7,} rows")
    print(f"  orders.csv    : {len(orders):>7,} rows")
    print(f"  TOTAL         : {total_rows:>7,} rows")
    print()
    print("Customer quality issues introduced:")
    print(f"  NULL email (completeness)         : {customer_issues['null_email']}")
    print(
        f"  Duplicate customer_id rows        : {customer_issues['duplicate_customer_id']} "
        f"(→ {customer_issues['duplicate_customer_id'] * 2} rows fail uniqueness)"
    )
    print()
    print("Order quality issues introduced:")
    print(f"  NULL customer_id (completeness)   : {order_issues['null_customer_id']}")
    print(f"  NULL product_id (completeness)    : {order_issues['null_product_id']}")
    print(f"  Orphan customer_id (referential)  : {order_issues['orphan_customer_id']} (99901–99950)")
    print(f"  Orphan product_id (referential)   : {order_issues['orphan_product_id']} (9901–9930)")
    print(
        f"  Duplicate order_id rows           : {order_issues['duplicate_order_id']} "
        f"(→ {order_issues['duplicate_order_id'] * 2} rows fail uniqueness)"
    )
    print()
    print(f"Total intentional issue injections  : {total_issues}")
    print(f"Approx. check-level defect rate     : {total_issues / total_rows * 100:.2f}%")
    print("=" * 60)


def main() -> None:
    """Generate products, customers, and orders CSV files with intentional DQ defects."""
    random.seed(RANDOM_SEED)
    fake = Faker()
    Faker.seed(RANDOM_SEED)

    print("Generating products...")
    products = generate_products(fake)
    product_price_lookup = {row["product_id"]: row["price"] for row in products}

    print("Generating customers...")
    customers = generate_customers(fake)
    customer_issues = apply_customer_quality_issues(customers)

    print("Generating orders...")
    orders = generate_orders(fake, product_price_lookup)
    order_issues = apply_order_quality_issues(orders)

    print("Writing CSV files...")
    write_csv(
        DATA_DIR / "products.csv",
        [
            "product_id",
            "product_name",
            "category",
            "price",
            "cost",
            "stock_quantity",
            "reorder_level",
        ],
        products,
    )
    write_csv(
        DATA_DIR / "customers.csv",
        [
            "customer_id",
            "customer_name",
            "email",
            "country",
            "signup_date",
            "customer_segment",
            "lifetime_value",
        ],
        customers,
    )
    write_csv(
        DATA_DIR / "orders.csv",
        [
            "order_id",
            "customer_id",
            "order_date",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status",
            "payment_date",
        ],
        orders,
    )

    print_summary(products, customers, orders, customer_issues, order_issues)


if __name__ == "__main__":
    main()
