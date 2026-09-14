"""
AtliQ Commerce — Phase 2
Live Order Event Producer — stream simulated order events to Kafka.

This producer:
1. Connects to Confluent Cloud Kafka
2. Generates simulated AtliQ order events
3. Publishes events to the Kafka topic
4. Uses order_id as the Kafka message key
5. Prints delivery success/failure
6. Flushes all buffered messages before exiting

Prerequisites:
    pip install confluent-kafka python-dotenv

.env file (next to this file):
    KAFKA_BOOTSTRAP=pkc-xxxxx.region.provider.confluent.cloud:9092
    KAFKA_API_KEY=...
    KAFKA_API_SECRET=...
    KAFKA_TOPIC=atliq.orders.events

Run:
    python order_event_producer.py --rate 2 --duration 300
"""

import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Producer


# Load variables from .env
load_dotenv()


# ---------------------------------------------------------
# Sample business data
# ---------------------------------------------------------

CITIES = [
    "Bengaluru",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
    "Ahmedabad",
    "Jaipur",
    "Surat",
]

METHODS = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Wallet",
    "COD",
]

PRODUCT_PRICES = {
    1: 2499,
    2: 3299,
    3: 1799,
    4: 1499,
    5: 4999,
    6: 899,
    7: 1299,
    8: 549,
    9: 749,
    10: 999,
    11: 599,
    12: 2199,
    13: 1099,
    14: 1599,
    15: 899,
    16: 299,
    17: 449,
    18: 549,
    19: 799,
    20: 1899,
    21: 249,
    22: 699,
    23: 799,
    24: 599,
    25: 499,
}


# ---------------------------------------------------------
# TODO 1 — Configure Kafka Producer
# ---------------------------------------------------------

def make_producer() -> Producer:
    """
    Create and return a Kafka Producer configured
    for Confluent Cloud.
    """

    config = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
    }

    return Producer(config)


# ---------------------------------------------------------
# Generate UTC timestamp
# ---------------------------------------------------------

def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z"
    )


# ---------------------------------------------------------
# Create an event
# ---------------------------------------------------------

def base_event(event_type: str, order: dict) -> dict:
    """
    Create one order event.

    event_id is unique and will later be used
    for de-duplication in the Silver streaming layer.
    """

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": now_iso(),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "city": order["city"],
        "product_id": order["product_id"],
        "quantity": order["quantity"],
        "order_amount": order["order_amount"],
        "payment_method": (
            order["payment_method"]
            if event_type == "payment_received"
            else None
        ),
    }


# ---------------------------------------------------------
# Generate a new order
# ---------------------------------------------------------

def new_order(order_id: int) -> dict:

    product_id = random.choice(list(PRODUCT_PRICES))
    qty = random.randint(1, 3)

    return {
        "order_id": order_id,
        "customer_id": random.randint(1, 40),
        "city": random.choice(CITIES),
        "product_id": product_id,
        "quantity": qty,
        "order_amount": PRODUCT_PRICES[product_id] * qty,
        "payment_method": random.choice(METHODS),
    }


# ---------------------------------------------------------
# Main producer logic
# ---------------------------------------------------------

def run(rate: float, duration: int):

    producer = make_producer()

    topic = os.environ.get(
        "KAFKA_TOPIC",
        "atliq.orders.events"
    )

    open_orders = []
    next_order_id = 100_000
    sent = 0

    deadline = time.time() + duration

    print(
        f"Producing to '{topic}' "
        f"at ~{rate} events/sec "
        f"for {duration}s ..."
    )

    try:

        while time.time() < deadline:

            roll = random.random()

            # -------------------------------------------------
            # Create a new order
            # -------------------------------------------------

            if roll < 0.55 or not open_orders:

                order = new_order(next_order_id)
                next_order_id += 1

                events = [
                    base_event(
                        "order_placed",
                        order
                    )
                ]

                # COD orders do not receive a payment_received
                # event immediately.
                if order["payment_method"] != "COD":

                    events.append(
                        base_event(
                            "payment_received",
                            order
                        )
                    )

                open_orders.append(order)

            # -------------------------------------------------
            # Ship an existing order
            # -------------------------------------------------

            elif roll < 0.90:

                order = open_orders.pop(
                    random.randrange(len(open_orders))
                )

                events = [
                    base_event(
                        "order_shipped",
                        order
                    )
                ]

            # -------------------------------------------------
            # Cancel an existing order
            # -------------------------------------------------

            else:

                order = open_orders.pop(
                    random.randrange(len(open_orders))
                )

                events = [
                    base_event(
                        "order_cancelled",
                        order
                    )
                ]

            # -------------------------------------------------
            # TODO 2 — Publish events to Kafka
            # -------------------------------------------------

            for ev in events:

                producer.produce(
                    topic,
                    key=str(ev["order_id"]),
                    value=json.dumps(ev),
                    callback=lambda err, msg: print(
                        (
                            f"Delivered to "
                            f"{msg.topic()} "
                            f"[{msg.partition()}] "
                            f"at offset {msg.offset()}"
                        )
                        if err is None
                        else
                        f"Delivery failed: {err}"
                    ),
                )

                sent += 1

            # Serve delivery callbacks
            producer.poll(0)

            # Control event generation rate
            time.sleep(1.0 / rate)

    except KeyboardInterrupt:

        print("\nStopping ...")

    finally:

        # -------------------------------------------------
        # TODO 3 — Guarantee delivery before exit
        # -------------------------------------------------

        producer.flush()

        print(
            f"Done. {sent} events sent."
        )


# ---------------------------------------------------------
# Command-line entry point
# ---------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Approximate events per second",
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=300,
        help="How long to produce events, in seconds",
    )

    args = parser.parse_args()

    run(
        args.rate,
        args.duration
    )