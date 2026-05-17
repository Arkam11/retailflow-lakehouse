"""
generate_customers.py
Simulates an OLTP customer database export with full load and CDC delta.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
random.seed(77)

CUSTOMER_TIERS       = ["Bronze", "Silver", "Gold", "Platinum"]
TIER_WEIGHTS         = [50, 30, 15, 5]
REGIONS              = ["North America", "Europe", "Asia Pacific", "Middle East", "South Asia"]
ACQUISITION_CHANNELS = ["organic_search", "paid_ads", "referral", "social_media", "email_campaign", "direct"]


def generate_customer(customer_id=None):
    signup_date = fake.date_time_between(start_date="-3y", end_date="-1d")
    return {
        "customer_id":         customer_id or f"CUST-{random.randint(1000, 9999)}",
        "first_name":          fake.first_name(),
        "last_name":           fake.last_name(),
        "email":               fake.email(),
        "phone":               fake.phone_number(),
        "date_of_birth":       str(fake.date_of_birth(minimum_age=18, maximum_age=75)),
        "gender":              random.choice(["M", "F", "Other", "Prefer not to say"]),
        "region":              random.choice(REGIONS),
        "country":             fake.country(),
        "city":                fake.city(),
        "postal_code":         fake.postcode(),
        "customer_tier":       random.choices(CUSTOMER_TIERS, weights=TIER_WEIGHTS, k=1)[0],
        "acquisition_channel": random.choice(ACQUISITION_CHANNELS),
        "signup_date":         signup_date.isoformat(),
        "last_login_date":     fake.date_time_between(start_date=signup_date, end_date="now").isoformat(),
        "is_active":           random.choices([True, False], weights=[85, 15], k=1)[0],
        "marketing_opt_in":    random.choice([True, False]),
        "lifetime_orders":     random.randint(0, 150),
        "lifetime_spend":      round(random.uniform(0, 15000), 2),
        "source_system":       "crm_database",
        "updated_at":          datetime.utcnow().isoformat(),
    }


def generate_full_customer_load(num_customers=500, output_dir="data_sources/sample_data"):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    customer_ids = [f"CUST-{1000 + i}" for i in range(num_customers)]
    customers    = [generate_customer(cid) for cid in customer_ids]
    timestamp    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename     = f"{output_dir}/customers_full_{timestamp}.json"

    with open(filename, "w") as f:
        for c in customers:
            f.write(json.dumps(c) + "\n")

    print(f"Generated {num_customers} customers (full load) -> {filename}")
    return filename


def generate_cdc_delta(num_changes=30, output_dir="data_sources/sample_data"):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    operations = ["INSERT", "UPDATE", "UPDATE", "UPDATE", "DELETE"]
    records    = []

    for _ in range(num_changes):
        operation   = random.choice(operations)
        customer_id = f"CUST-{random.randint(1000, 1499)}"
        customer    = generate_customer(customer_id)
        records.append({
            "_cdc_operation": operation,
            "_cdc_timestamp": datetime.utcnow().isoformat(),
            "_cdc_source":    "sqlserver_cdc",
            **customer,
        })

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename  = f"{output_dir}/customers_cdc_{timestamp}.json"

    with open(filename, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    ops_summary = {op: sum(1 for r in records if r["_cdc_operation"] == op) for op in ["INSERT", "UPDATE", "DELETE"]}
    print(f"Generated CDC delta: {ops_summary} -> {filename}")
    return filename


if __name__ == "__main__":
    generate_full_customer_load(num_customers=500)
    generate_cdc_delta(num_changes=30)
