"""
test_generators.py
Basic smoke tests for all data source generators.
Verifies each generator produces output files with correct structure.
"""

import json
import os
import csv
import pytest
from pathlib import Path


OUTPUT_DIR = "data_sources/sample_data"


def read_jsonl(filepath):
    """Read a JSON Lines file and return list of records."""
    records = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def latest_file(prefix, extension):
    """Find the most recently created file matching prefix and extension."""
    files = list(Path(OUTPUT_DIR).glob(f"{prefix}*.{extension}"))
    assert len(files) > 0, f"No {prefix}*.{extension} files found in {OUTPUT_DIR}"
    return max(files, key=os.path.getctime)


class TestSalesGenerator:
    def test_sales_file_exists(self):
        from data_sources.generators.generate_sales import generate_sales_batch
        path = generate_sales_batch(num_orders=10, output_dir=OUTPUT_DIR)
        assert Path(path).exists()

    def test_sales_record_structure(self):
        path   = latest_file("sales_orders", "json")
        record = read_jsonl(path)[0]
        required_fields = [
            "order_id", "customer_id", "order_date", "order_status",
            "region", "total_amount", "payment_method", "items",
            "source_system", "extracted_at"
        ]
        for field in required_fields:
            assert field in record, f"Missing field: {field}"

    def test_sales_items_are_list(self):
        path   = latest_file("sales_orders", "json")
        record = read_jsonl(path)[0]
        assert isinstance(record["items"], list)
        assert len(record["items"]) >= 1

    def test_sales_total_is_positive(self):
        path    = latest_file("sales_orders", "json")
        records = read_jsonl(path)
        for r in records:
            assert r["total_amount"] > 0, f"Order {r['order_id']} has non-positive total"


class TestInventoryGenerator:
    def test_inventory_file_exists(self):
        from data_sources.generators.generate_inventory import generate_inventory_snapshot
        path = generate_inventory_snapshot(output_dir=OUTPUT_DIR)
        assert Path(path).exists()

    def test_inventory_has_correct_row_count(self):
        path = latest_file("inventory_snapshot", "csv")
        with open(path) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 50, f"Expected 50 rows (5 warehouses x 10 products), got {len(rows)}"

    def test_inventory_reorder_flag_logic(self):
        path = latest_file("inventory_snapshot", "csv")
        with open(path) as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            available = int(row["available_quantity"])
            reorder   = int(row["reorder_point"])
            flag      = row["reorder_flag"]
            expected  = "Y" if available < reorder else "N"
            assert flag == expected, f"Reorder flag wrong for {row['product_id']} at {row['warehouse_id']}"


class TestCustomerGenerator:
    def test_full_load_file_exists(self):
        from data_sources.generators.generate_customers import generate_full_customer_load
        path = generate_full_customer_load(num_customers=50, output_dir=OUTPUT_DIR)
        assert Path(path).exists()

    def test_customer_record_structure(self):
        path   = latest_file("customers_full", "json")
        record = read_jsonl(path)[0]
        required_fields = [
            "customer_id", "first_name", "last_name", "email",
            "region", "customer_tier", "signup_date",
            "lifetime_orders", "lifetime_spend", "source_system"
        ]
        for field in required_fields:
            assert field in record, f"Missing field: {field}"

    def test_cdc_operations_are_valid(self):
        from data_sources.generators.generate_customers import generate_cdc_delta
        path    = generate_cdc_delta(num_changes=20, output_dir=OUTPUT_DIR)
        records = read_jsonl(path)
        valid_ops = {"INSERT", "UPDATE", "DELETE"}
        for r in records:
            assert r["_cdc_operation"] in valid_ops, f"Invalid CDC op: {r['_cdc_operation']}"
        ops_present = {r["_cdc_operation"] for r in records}
        assert len(ops_present) > 1, "CDC delta should contain multiple operation types"


class TestWeatherGenerator:
    def test_weather_file_exists(self):
        from data_sources.generators.weather_api_client import collect_weather_for_all_cities
        path = collect_weather_for_all_cities(output_dir=OUTPUT_DIR)
        assert Path(path).exists()

    def test_weather_has_all_cities(self):
        path    = latest_file("weather", "json")
        records = read_jsonl(path)
        assert len(records) == 5, f"Expected 5 city records, got {len(records)}"

    def test_weather_record_structure(self):
        path   = latest_file("weather", "json")
        record = read_jsonl(path)[0]
        required_fields = [
            "weather_date", "city", "country", "region",
            "temp_celsius", "humidity_pct", "condition", "source_system"
        ]
        for field in required_fields:
            assert field in record, f"Missing field: {field}"
