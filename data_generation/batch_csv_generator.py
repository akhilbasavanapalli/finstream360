"""
FinStream360 - Batch CSV / Parquet Seed Generator
==================================================
Generates reference datasets (customers, merchants) and a seed transaction
history so the pipeline can be bootstrapped without a live Kafka cluster.

Usage:
    python batch_csv_generator.py --rows 500000 --output-dir ./sample_data

Author : Akhil Basavanapalli
Tech   : Python, Pandas, PyArrow, Faker
"""

import argparse
import logging
import os
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("batch_generator")

MERCHANT_CATEGORIES = [
    "GROCERY_STORE",
    "GAS_STATION",
    "RESTAURANT",
    "ONLINE_RETAIL",
    "TRAVEL",
    "ENTERTAINMENT",
    "HEALTHCARE",
    "UTILITIES",
    "ATM_WITHDRAWAL",
]
CARD_TYPES = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]
US_STATES = ["TX", "CA", "NY", "FL", "GA", "IL", "PA", "OH", "NC", "MI"]
FRAUD_RATE = 0.02


# ─────────────────────────────────────────────────────────────────────────────
def generate_customers(n: int = 50_000) -> pd.DataFrame:
    log.info("Generating %d customer records …", n)
    records = []
    for _ in range(n):
        records.append(
            {
                "customer_id": str(uuid.uuid4()),
                "full_name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "home_state": random.choice(US_STATES),
                "credit_score": random.randint(580, 850),
                "card_type": random.choice(CARD_TYPES),
                "card_last4": str(random.randint(1000, 9999)),
                "credit_limit_usd": random.choice([5_000, 10_000, 15_000, 25_000, 50_000]),
                "account_open_date": fake.date_between(start_date="-10y", end_date="-30d").isoformat(),
                "is_active": True,
                "created_at": datetime.utcnow().isoformat(),
            }
        )
    return pd.DataFrame(records)


def generate_merchants(n: int = 5_000) -> pd.DataFrame:
    log.info("Generating %d merchant records …", n)
    records = []
    for _ in range(n):
        records.append(
            {
                "merchant_id": str(uuid.uuid4()),
                "merchant_name": fake.company(),
                "category": random.choice(MERCHANT_CATEGORIES),
                "state": random.choice(US_STATES),
                "city": fake.city(),
                "zip_code": fake.zipcode(),
                "is_high_risk": random.random() < 0.05,
                "registration_date": fake.date_between(start_date="-5y", end_date="-1d").isoformat(),
                "created_at": datetime.utcnow().isoformat(),
            }
        )
    return pd.DataFrame(records)


def generate_transactions(customers_df: pd.DataFrame, n: int = 500_000) -> pd.DataFrame:
    log.info("Generating %d transaction records …", n)
    customer_ids = customers_df["customer_id"].tolist()
    customer_map = customers_df.set_index("customer_id").to_dict("index")

    records = []
    start_date = datetime.utcnow() - timedelta(days=90)

    for _ in range(n):
        cid = random.choice(customer_ids)
        cust = customer_map[cid]
        cat = random.choice(MERCHANT_CATEGORIES)

        amount_ranges = {
            "GROCERY_STORE": (10, 350),
            "GAS_STATION": (30, 120),
            "RESTAURANT": (15, 250),
            "ONLINE_RETAIL": (5, 2500),
            "TRAVEL": (200, 8000),
            "ENTERTAINMENT": (20, 500),
            "HEALTHCARE": (50, 5000),
            "UTILITIES": (50, 400),
            "ATM_WITHDRAWAL": (20, 800),
        }
        lo, hi = amount_ranges.get(cat, (10, 500))
        amount = round(random.uniform(lo, hi), 2)
        is_fraud = random.random() < FRAUD_RATE or (amount > cust["credit_limit_usd"] * 0.85 and random.random() < 0.30)

        txn_date = start_date + timedelta(
            seconds=random.randint(0, int((datetime.utcnow() - start_date).total_seconds()))
        )

        records.append(
            {
                "transaction_id": str(uuid.uuid4()),
                "customer_id": cid,
                "card_type": cust["card_type"],
                "card_last4": cust["card_last4"],
                "merchant_name": fake.company(),
                "merchant_category": cat,
                "merchant_state": random.choice(US_STATES) if is_fraud else cust["home_state"],
                "amount_usd": amount,
                "currency": "USD",
                "transaction_ts": txn_date.isoformat() + "Z",
                "is_fraud": is_fraud,
                "fraud_reason": "SYNTHETIC_INJECTION" if is_fraud else None,
                "card_present": not is_fraud,
                "response_code": "00",
                "created_at": datetime.utcnow().isoformat(),
            }
        )

    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="FinStream360 seed data generator")
    parser.add_argument("--rows", type=int, default=500_000, help="Transaction row count")
    parser.add_argument("--customers", type=int, default=50_000, help="Customer count")
    parser.add_argument("--merchants", type=int, default=5_000, help="Merchant count")
    parser.add_argument("--output-dir", type=str, default="./sample_data")
    parser.add_argument("--format", choices=["csv", "parquet", "both"], default="parquet")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    customers_df = generate_customers(args.customers)
    merchants_df = generate_merchants(args.merchants)
    transactions_df = generate_transactions(customers_df, args.rows)

    frames = {
        "customers": customers_df,
        "merchants": merchants_df,
        "transactions": transactions_df,
    }

    for name, df in frames.items():
        if args.format in ("parquet", "both"):
            path = os.path.join(args.output_dir, f"{name}.parquet")
            df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
            log.info("Wrote %s rows → %s", len(df), path)
        if args.format in ("csv", "both"):
            path = os.path.join(args.output_dir, f"{name}.csv")
            df.to_csv(path, index=False)
            log.info("Wrote %s rows → %s", len(df), path)

    log.info("Seed data generation complete.")
    log.info("Transaction fraud rate: %.2f%%", 100 * transactions_df["is_fraud"].sum() / len(transactions_df))


if __name__ == "__main__":
    main()
