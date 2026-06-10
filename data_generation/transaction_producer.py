"""
FinStream360 - Real-Time Transaction Producer
=============================================
Simulates a high-volume credit card transaction stream using Apache Kafka.
Publishes synthetic transactions with realistic patterns including fraud signals.

Author : Akhil Basavanapalli
Tech   : Python, Kafka (confluent-kafka), Faker, Pandas
"""

import json
import time
import random
import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from faker import Faker
from confluent_kafka import Producer

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_TRANSACTIONS       = "raw_transactions"
TOPIC_CUSTOMERS          = "raw_customers"
TRANSACTIONS_PER_SECOND  = 50        # tune for load testing
FRAUD_RATE               = 0.02      # 2% fraud injection rate

fake = Faker()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("transaction_producer")

# ── Reference data ───────────────────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "GROCERY_STORE", "GAS_STATION", "RESTAURANT", "ONLINE_RETAIL",
    "TRAVEL", "ENTERTAINMENT", "HEALTHCARE", "UTILITIES", "ATM_WITHDRAWAL"
]

CARD_TYPES = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]

US_STATES = [
    "TX", "CA", "NY", "FL", "GA", "IL", "PA", "OH", "NC", "MI",
    "WA", "AZ", "CO", "TN", "IN", "MO", "MD", "WI", "MN", "SC"
]

# Pre-generate a pool of customers to simulate repeat transactions
CUSTOMER_POOL_SIZE = 10_000
customer_pool = [
    {
        "customer_id":  str(uuid.uuid4()),
        "name":         fake.name(),
        "email":        fake.email(),
        "phone":        fake.phone_number(),
        "state":        random.choice(US_STATES),
        "credit_score": random.randint(580, 850),
        "card_type":    random.choice(CARD_TYPES),
        "card_number":  "**** **** **** " + str(random.randint(1000, 9999)),
        "credit_limit": random.choice([5_000, 10_000, 15_000, 25_000, 50_000]),
        "account_age_months": random.randint(1, 120),
    }
    for _ in range(CUSTOMER_POOL_SIZE)
]

log.info("Customer pool generated: %d customers", len(customer_pool))


# ── Helpers ───────────────────────────────────────────────────────────────────
def is_fraud(customer: Dict, amount: float) -> bool:
    """Inject fraud based on amount thresholds + random noise."""
    if random.random() < FRAUD_RATE:
        return True
    if amount > customer["credit_limit"] * 0.9:
        return random.random() < 0.30
    return False


def generate_transaction(customer: Dict) -> Dict[str, Any]:
    """Create one synthetic credit card transaction record."""
    category = random.choice(MERCHANT_CATEGORIES)

    # Amount distribution varies by category
    amount_ranges = {
        "GROCERY_STORE":   (10,   350),
        "GAS_STATION":     (30,   120),
        "RESTAURANT":      (15,   250),
        "ONLINE_RETAIL":   (5,  2_500),
        "TRAVEL":          (200, 8_000),
        "ENTERTAINMENT":   (20,   500),
        "HEALTHCARE":      (50, 5_000),
        "UTILITIES":       (50,   400),
        "ATM_WITHDRAWAL":  (20,   800),
    }
    lo, hi = amount_ranges.get(category, (10, 500))
    amount = round(random.uniform(lo, hi), 2)

    fraud_flag = is_fraud(customer, amount)

    # Fraud transactions often happen late at night or in a different state
    if fraud_flag:
        txn_state = random.choice(US_STATES)   # different state
        hour = random.choice([0, 1, 2, 3, 23])  # late night
    else:
        txn_state = customer["state"]
        hour = random.randint(6, 22)

    txn_time = datetime.utcnow().replace(hour=hour, minute=random.randint(0, 59))

    return {
        "transaction_id":   str(uuid.uuid4()),
        "customer_id":      customer["customer_id"],
        "card_type":        customer["card_type"],
        "card_last4":       customer["card_number"][-4:],
        "merchant_name":    fake.company(),
        "merchant_category":category,
        "merchant_state":   txn_state,
        "amount_usd":       amount,
        "currency":         "USD",
        "transaction_ts":   txn_time.isoformat() + "Z",
        "is_fraud":         fraud_flag,
        "fraud_reason":     "AMOUNT_ANOMALY" if fraud_flag else None,
        "card_present":     not fraud_flag,   # CNP more common in fraud
        "response_code":    "00",             # approved
        "event_created_at": datetime.utcnow().isoformat() + "Z",
    }


# ── Kafka delivery callback ────────────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        log.error("Message delivery failed: %s", err)
    else:
        log.debug("Delivered to %s [%d] @ offset %d",
                  msg.topic(), msg.partition(), msg.offset())


# ── Main producer loop ────────────────────────────────────────────────────
def run_producer():
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id":         "finstream360-producer",
        "acks":              "all",
        "retries":           3,
        "linger.ms":         5,
        "batch.size":        65536,
        "compression.type":  "snappy",
    }

    producer = Producer(producer_conf)
    log.info("Producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("Streaming transactions to topic '%s' at ~%d TPS ...",
             TOPIC_TRANSACTIONS, TRANSACTIONS_PER_SECOND)

    total_sent  = 0
    fraud_count = 0
    interval    = 1.0 / TRANSACTIONS_PER_SECOND

    try:
        while True:
            customer = random.choice(customer_pool)
            txn      = generate_transaction(customer)

            producer.produce(
                topic=TOPIC_TRANSACTIONS,
                key=txn["customer_id"],
                value=json.dumps(txn),
                callback=delivery_report,
            )
            producer.poll(0)

            total_sent += 1
            if txn["is_fraud"]:
                fraud_count += 1

            if total_sent % 1_000 == 0:
                log.info(
                    "Sent: %d  |  Fraud injected: %d (%.1f%%)",
                    total_sent, fraud_count, 100 * fraud_count / total_sent
                )

            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("Producer stopped. Total sent: %d", total_sent)
    finally:
        producer.flush()
        log.info("Producer flushed and closed.")


if __name__ == "__main__":
    run_producer()
