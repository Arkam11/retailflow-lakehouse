"""
generate_sales.py
Simulates a retail e-commerce REST API returning order data.
In production this would be replaced by actual API calls to
an order management system (OMS) like Salesforce Commerce Cloud.

What this teaches:
- Realistic JSON data structures matching real API responses
- Incremental data generation (new orders since last run)
- Data variety: multiple statuses, payment methods, regions
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
random.seed(42)  # Reproducible data for testing

# ── Constants that mirror real retail domain values ──────────────────────────
ORDER_STATUSES   = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
PAYMENT_METHODS  = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
REGIONS          = ["North America", "Europe", "Asia Pacific", "Middle East", "South Asia"]
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food & Beverage"]

PRODUCTS = [
    {"product_id": "PRD-001", "name": "Wireless Headphones",   "category": "Electronics",    "unit_price": 89.99},
    {"product_id": "PRD-002", "name": "Running Shoes",          "category": "Sports",         "unit_price": 119.99},
    {"product_id": "PRD-003", "name": "Coffee Maker",           "category": "Home & Garden",  "unit_price": 59.99},
    {"product_id": "PRD-004", "name": "Python Programming Book","category": "Books",           "unit_price": 39.99},
    {"product_id": "PRD-005", "name": "Yoga Mat",               "category": "Sports",         "unit_price": 29.99},
    {"product_id": "PRD-006", "name": "Smart Watch",            "category": "Electronics",    "unit_price": 199.99},
    {"product_id": "PRD-007", "name": "Winter Jacket",          "category": "Clothing",       "unit_price": 149.99},
    {"product_id": "PRD-008", "name": "Board Game",             "category": "Toys",           "unit_price": 44.99},
    {"product_id": "PRD-009", "name": "Protein Powder",         "category": "Food & Beverage","unit_price": 54.99},
    {"product_id": "PRD-010", "name": "Desk Lamp",              "category": "Home & Garden",  "unit_price": 34.99},
]


def generate_order_item(order_id: str) -> dict:
    """Generate a single line item within an order."""
    product  = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    discount = round(random.choice([0, 0, 0, 5, 10, 15, 20]) / 100, 2)  # 70% chance no discount
    unit_price_after_discount = round(product["unit_price"] * (1 - discount), 2)

    return {
        "line_item_id":    str(uuid.uuid4()),
        "order_id":        order_id,
        "product_id":      product["product_id"],
        "product_name":    product["name"],
        "category":        product["category"],
        "quantity":        quantity,
        "unit_price":      product["unit_price"],
        "discount_pct":    discount,
        "line_total":      round(unit_price_after_discount * quantity, 2),
    }


def generate_order(order_date: datetime = None) -> dict:
    """
    Generate a single realistic order record.
    Mirrors what a real OMS REST API would return in its response body.
    """
    order_id     = f"ORD-{str(uuid.uuid4())[:8].upper()}"
    order_date   = order_date or fake.date_time_between(start_date="-90d", end_date="now")
    num_items    = random.randint(1, 4)
    items        = [generate_order_item(order_id) for _ in range(num_items)]
    subtotal     = round(sum(i["line_total"] for i in items), 2)
    tax_rate     = 0.08
    shipping_fee = round(random.choice([0, 4.99, 9.99, 14.99]), 2)
    total        = round(subtotal + (subtotal * tax_rate) + shipping_fee, 2)
    status       = random.choices(
        ORDER_STATUSES,
        weights=[10, 20, 25, 35, 7, 3],  # Realistic distribution — most orders deliver
        k=1
    )[0]

    return {
        # ── Core order fields ───────────────────────────────────────────────
        "order_id":        order_id,
        "customer_id":     f"CUST-{random.randint(1000, 9999)}",
        "order_date":      order_date.isoformat(),
        "order_status":    status,
        "region":          random.choice(REGIONS),
        "country":         fake.country(),
        "city":            fake.city(),
        # ── Financial fields ────────────────────────────────────────────────
        "currency":        "USD",
        "subtotal":        subtotal,
        "tax_amount":      round(subtotal * tax_rate, 2),
        "shipping_fee":    shipping_fee,
        "total_amount":    total,
        "payment_method":  random.choice(PAYMENT_METHODS),
        # ── Line items (nested — mirrors real API response structure) ────────
        "items":           items,
        "item_count":      num_items,
        # ── Metadata fields (what Bronze layer will use for audit) ───────────
        "source_system":   "ecommerce_oms",
        "api_version":     "v2.1",
        "extracted_at":    datetime.utcnow().isoformat(),
    }


def generate_sales_batch(num_orders: int = 100, output_dir: str = "data_sources/sample_data") -> str:
    """
    Generate a batch of orders and save as JSON.
    Simulates what a scheduled API pull would return each day.

    Args:
        num_orders: How many orders to generate
        output_dir: Where to save the output file

    Returns:
        Path to the generated file
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    orders    = [generate_order() for _ in range(num_orders)]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename  = f"{output_dir}/sales_orders_{timestamp}.json"

    # Write as JSON Lines format (one record per line)
    # This is what real APIs return and what Spark reads most efficiently
    with open(filename, "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\n")

    print(f"✓ Generated {num_orders} orders → {filename}")
    return filename


def generate_incremental_sales(hours_back: int = 1, output_dir: str = "data_sources/sample_data") -> str:
    """
    Simulates incremental API pull — only orders from the last N hours.
    This is how real metadata-driven pipelines work:
    they pull only NEW data since the last watermark timestamp.
    """
    num_orders = random.randint(10, 50)  # Realistic hourly volume
    now        = datetime.utcnow()
    orders     = [
        generate_order(
            order_date=now - timedelta(hours=random.uniform(0, hours_back))
        )
        for _ in range(num_orders)
    ]

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename  = f"{output_dir}/sales_incremental_{timestamp}.json"

    with open(filename, "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\n")

    print(f"✓ Generated {num_orders} incremental orders (last {hours_back}h) → {filename}")
    return filename


# ── Run directly to test ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating sales data...")
    generate_sales_batch(num_orders=200)
    generate_incremental_sales(hours_back=1)
    print("Done.")
