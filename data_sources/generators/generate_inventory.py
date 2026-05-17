"""
generate_inventory.py
Simulates a daily warehouse batch export as CSV.
"""

import csv
import random
from datetime import datetime
from pathlib import Path
from faker import Faker

fake = Faker()
random.seed(99)

WAREHOUSES = [
    {"warehouse_id": "WH-001", "name": "London Central",     "region": "Europe",        "country": "UK"},
    {"warehouse_id": "WH-002", "name": "New York East",       "region": "North America", "country": "USA"},
    {"warehouse_id": "WH-003", "name": "Dubai Logistics",     "region": "Middle East",   "country": "UAE"},
    {"warehouse_id": "WH-004", "name": "Singapore Hub",       "region": "Asia Pacific",  "country": "Singapore"},
    {"warehouse_id": "WH-005", "name": "Mumbai Distribution", "region": "South Asia",    "country": "India"},
]

PRODUCTS = [
    {"product_id": "PRD-001", "name": "Wireless Headphones",    "category": "Electronics",     "reorder_point": 50},
    {"product_id": "PRD-002", "name": "Running Shoes",           "category": "Sports",          "reorder_point": 30},
    {"product_id": "PRD-003", "name": "Coffee Maker",            "category": "Home & Garden",   "reorder_point": 20},
    {"product_id": "PRD-004", "name": "Python Programming Book", "category": "Books",            "reorder_point": 15},
    {"product_id": "PRD-005", "name": "Yoga Mat",                "category": "Sports",          "reorder_point": 40},
    {"product_id": "PRD-006", "name": "Smart Watch",             "category": "Electronics",     "reorder_point": 25},
    {"product_id": "PRD-007", "name": "Winter Jacket",           "category": "Clothing",        "reorder_point": 35},
    {"product_id": "PRD-008", "name": "Board Game",              "category": "Toys",            "reorder_point": 20},
    {"product_id": "PRD-009", "name": "Protein Powder",          "category": "Food & Beverage", "reorder_point": 45},
    {"product_id": "PRD-010", "name": "Desk Lamp",               "category": "Home & Garden",   "reorder_point": 30},
]

FIELDNAMES = [
    "snapshot_date", "warehouse_id", "warehouse_name", "region", "country",
    "product_id", "product_name", "category",
    "stock_quantity", "reserved_quantity", "available_quantity",
    "reorder_point", "reorder_flag", "days_of_supply",
    "last_received_date", "last_sold_date",
    "unit_cost", "total_inventory_value",
    "source_system", "extracted_at",
]


def generate_inventory_snapshot(output_dir="data_sources/sample_data"):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    today     = datetime.utcnow().date()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename  = f"{output_dir}/inventory_snapshot_{timestamp}.csv"
    rows      = []

    for warehouse in WAREHOUSES:
        for product in PRODUCTS:
            stock_qty     = random.randint(0, 500)
            reserved_qty  = random.randint(0, min(stock_qty, 50))
            available_qty = stock_qty - reserved_qty
            unit_cost     = round(random.uniform(10, 150), 2)
            days_supply   = round(available_qty / max(random.randint(1, 20), 1), 1)

            rows.append({
                "snapshot_date":         str(today),
                "warehouse_id":          warehouse["warehouse_id"],
                "warehouse_name":        warehouse["name"],
                "region":                warehouse["region"],
                "country":               warehouse["country"],
                "product_id":            product["product_id"],
                "product_name":          product["name"],
                "category":              product["category"],
                "stock_quantity":        stock_qty,
                "reserved_quantity":     reserved_qty,
                "available_quantity":    available_qty,
                "reorder_point":         product["reorder_point"],
                "reorder_flag":          "Y" if available_qty < product["reorder_point"] else "N",
                "days_of_supply":        days_supply,
                "last_received_date":    str(fake.date_between(start_date="-30d", end_date="today")),
                "last_sold_date":        str(fake.date_between(start_date="-7d",  end_date="today")),
                "unit_cost":             unit_cost,
                "total_inventory_value": round(stock_qty * unit_cost, 2),
                "source_system":         "warehouse_wms",
                "extracted_at":          datetime.utcnow().isoformat(),
            })

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} inventory rows -> {filename}")
    return filename


if __name__ == "__main__":
    generate_inventory_snapshot()
