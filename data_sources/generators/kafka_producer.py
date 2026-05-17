"""
kafka_producer.py
Simulates a live order event stream published to Kafka.
Events represent real-time order lifecycle changes:
order_placed → payment_processed → order_shipped → order_delivered

In production this would be your microservices publishing domain events.
Here we simulate continuous event flow for Spark Structured Streaming to consume.

What this teaches:
- Kafka producer setup and configuration
- Event-driven architecture patterns
- Real-time data generation for streaming pipelines
"""

import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_ORDERS            = "retail-orders"
TOPIC_INVENTORY_ALERTS  = "retail-inventory-alerts"

EVENT_TYPES = [
    "order_placed",
    "payment_processed",
    "order_confirmed",
    "order_shipped",
    "order_delivered",
    "order_cancelled",
    "return_initiated",
]

PRODUCTS = [
    {"product_id": "PRD-001", "name": "Wireless Headphones",  "price": 89.99},
    {"product_id": "PRD-002", "name": "Running Shoes",         "price": 119.99},
    {"product_id": "PRD-003", "name": "Coffee Maker",          "price": 59.99},
    {"product_id": "PRD-006", "name": "Smart Watch",           "price": 199.99},
    {"product_id": "PRD-007", "name": "Winter Jacket",         "price": 149.99},
]


def create_producer() -> KafkaProducer:
    """Create and return a configured Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Reliability settings
        acks="all",           # Wait for all replicas to acknowledge
        retries=3,
        retry_backoff_ms=300,
    )


def generate_order_event() -> dict:
    """Generate a single real-time order event."""
    product    = random.choice(PRODUCTS)
    quantity   = random.randint(1, 3)
    event_type = random.choices(
        EVENT_TYPES,
        weights=[30, 25, 20, 15, 5, 3, 2],
        k=1
    )[0]

    return {
        "event_id":      str(uuid.uuid4()),
        "event_type":    event_type,
        "event_timestamp": datetime.utcnow().isoformat(),
        "order_id":      f"ORD-{str(uuid.uuid4())[:8].upper()}",
        "customer_id":   f"CUST-{random.randint(1000, 1499)}",
        "product_id":    product["product_id"],
        "product_name":  product["name"],
        "quantity":      quantity,
        "unit_price":    product["price"],
        "total_amount":  round(product["price"] * quantity, 2),
        "region":        random.choice(["North America", "Europe", "Asia Pacific"]),
        "payment_method": random.choice(["credit_card", "paypal", "apple_pay"]),
        "source_system": "order_events_stream",
    }


def generate_inventory_alert() -> dict:
    """Generate a low-stock inventory alert event."""
    product = random.choice(PRODUCTS)
    return {
        "alert_id":        str(uuid.uuid4()),
        "alert_type":      "low_stock",
        "alert_timestamp": datetime.utcnow().isoformat(),
        "warehouse_id":    f"WH-00{random.randint(1, 5)}",
        "product_id":      product["product_id"],
        "product_name":    product["name"],
        "current_stock":   random.randint(0, 10),
        "reorder_point":   20,
        "severity":        random.choice(["warning", "critical"]),
        "source_system":   "inventory_alerts_stream",
    }


def run_producer(events_per_second: float = 1.0, duration_seconds: int = 60):
    """
    Run the Kafka producer for a set duration.

    Args:
        events_per_second: Rate of event generation
        duration_seconds:  How long to run (0 = run forever)
    """
    print(f"Starting Kafka producer → topic: {TOPIC_ORDERS}")
    print(f"Rate: {events_per_second} events/sec | Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print("Press Ctrl+C to stop\n")

    try:
        producer   = create_producer()
        count      = 0
        start_time = time.time()
        sleep_time = 1.0 / events_per_second

        while True:
            # Send order event
            event = generate_order_event()
            producer.send(
                topic=TOPIC_ORDERS,
                key=event["order_id"],       # Partition by order_id for ordering guarantees
                value=event,
            )

            # Every 10th event also send an inventory alert
            if count % 10 == 0:
                alert = generate_inventory_alert()
                producer.send(
                    topic=TOPIC_INVENTORY_ALERTS,
                    key=alert["warehouse_id"],
                    value=alert,
                )

            count += 1
            print(f"  [{count}] {event['event_type']} | {event['order_id']} | ${event['total_amount']}")

            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                print(f"\n✓ Completed: {count} events in {duration_seconds}s")
                break

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n✓ Stopped by user. Total events sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    # Default: 1 event per second for 60 seconds
    # Change events_per_second to stress test your streaming pipeline
    run_producer(events_per_second=1.0, duration_seconds=60)
