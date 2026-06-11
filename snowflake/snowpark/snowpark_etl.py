"""
FinStream360 · Snowpark Python ETL
===================================
Demonstrates Snowpark API for data wrangling and transformation directly
within Snowflake — no data movement, compute runs inside the warehouse.

Mirrors the PySpark Silver-layer logic but executed natively in Snowflake.

Author : Akhil Basavanapalli
Tech   : Snowpark API, Python, Pandas on Snowpark
"""

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
    IntegerType,
)
import pandas as pd
import logging
import os
from typing import Optional

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("snowpark_etl")

# ── Connection params (pull from env / Secrets Manager in prod) ───────────────
CONNECTION_PARAMS = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", "your_account"),
    "user": os.environ.get("SNOWFLAKE_USER", "your_user"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", "your_password"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    "warehouse": "FINSTREAM360_WH",
    "database": "FINSTREAM360",
    "schema": "STAGING",
}


def get_session() -> Session:
    """Build and return a Snowpark session."""
    session = Session.builder.configs(CONNECTION_PARAMS).create()
    log.info("Snowpark session created: %s", session.get_current_warehouse())
    return session


# ─────────────────────────────────────────────────────────────────────────────
# 1 · Data Quality Checks using Snowpark DataFrame API
# ─────────────────────────────────────────────────────────────────────────────
def run_dq_checks(session: Session) -> dict:
    """
    Run data quality assertions on staging transactions.
    Returns a dict of {check_name: pass/fail}.
    """
    txn_df = session.table("STAGING.STG_TRANSACTIONS")
    total = txn_df.count()

    checks = {
        "total_rows": total,
        "null_transaction_ids": txn_df.filter(F.col("TRANSACTION_ID").isNull()).count(),
        "null_customer_ids": txn_df.filter(F.col("CUSTOMER_ID").isNull()).count(),
        "negative_amounts": txn_df.filter(F.col("AMOUNT_USD") <= 0).count(),
        "future_transactions": txn_df.filter(F.col("TRANSACTION_TS") > F.current_timestamp()).count(),
        "duplicates": total - txn_df.select("TRANSACTION_ID").distinct().count(),
    }

    all_pass = all(v == 0 for k, v in checks.items() if k != "total_rows")
    checks["all_passed"] = all_pass

    log.info("DQ results: %s", checks)
    return checks


# ─────────────────────────────────────────────────────────────────────────────
# 2 · Wrangling: Normalise and Enrich via Snowpark
# ─────────────────────────────────────────────────────────────────────────────
def wrangle_transactions(session: Session):
    """
    Read staging transactions, apply cleansing, add derived columns,
    and write back to a wrangled temp table.
    Uses Pandas on Snowpark for complex transformations that need UDF-style logic.
    """
    txn_df = session.table("STAGING.STG_TRANSACTIONS")

    wrangled = (
        txn_df.withColumn("MERCHANT_CATEGORY", F.upper(F.trim(F.col("MERCHANT_CATEGORY"))))
        .withColumn("MERCHANT_STATE", F.upper(F.trim(F.col("MERCHANT_STATE"))))
        .withColumn("CARD_TYPE", F.upper(F.trim(F.col("CARD_TYPE"))))
        .withColumn("CURRENCY", F.upper(F.trim(F.col("CURRENCY"))))
        .withColumn("TXN_HOUR", F.hour(F.col("TRANSACTION_TS")))
        .withColumn("TXN_DAY_OF_WEEK", F.dayofweek(F.col("TRANSACTION_TS")))
        .withColumn("TXN_MONTH", F.month(F.col("TRANSACTION_TS")))
        .withColumn("TXN_YEAR", F.year(F.col("TRANSACTION_TS")))
        .withColumn("IS_WEEKEND", F.col("TXN_DAY_OF_WEEK").isin(1, 7))
        .withColumn("IS_LATE_NIGHT", F.col("TXN_HOUR").between(0, 5))
        .withColumn(
            "DQ_PASSED",
            F.col("TRANSACTION_ID").isNotNull() & F.col("CUSTOMER_ID").isNotNull() & (F.col("AMOUNT_USD") > 0),
        )
    )

    row_count = wrangled.count()
    log.info("Wrangled %d transactions", row_count)

    # Write to Silver schema
    wrangled.write.mode("overwrite").save_as_table("SILVER.TRANSACTIONS_WRANGLED")
    log.info("Written to SILVER.TRANSACTIONS_WRANGLED")
    return row_count


# ─────────────────────────────────────────────────────────────────────────────
# 3 · Snowpark Pandas: Customer Segment Analysis
# ─────────────────────────────────────────────────────────────────────────────
def compute_customer_segments(session: Session) -> pd.DataFrame:
    """
    Use Snowpark Pandas to compute RFM (Recency-Frequency-Monetary) segments.
    Returns a Pandas DataFrame for downstream reporting / Power BI export.
    """
    import snowflake.snowpark.modin.plugin  # noqa  activate Snowpark pandas

    spd = session.table("GOLD.CUSTOMER_360").to_pandas()  # pull to local Pandas

    # RFM scoring
    spd["recency_score"] = pd.qcut(spd["LAST_TXN_TS"].rank(ascending=False), q=4, labels=[4, 3, 2, 1]).astype(int)
    spd["frequency_score"] = pd.qcut(spd["TOTAL_TRANSACTIONS"].rank(), q=4, labels=[1, 2, 3, 4]).astype(int)
    spd["monetary_score"] = pd.qcut(spd["LIFETIME_SPEND_USD"].rank(), q=4, labels=[1, 2, 3, 4]).astype(int)

    spd["rfm_score"] = (
        spd["recency_score"].astype(str) + spd["frequency_score"].astype(str) + spd["monetary_score"].astype(str)
    )

    def rfm_segment(row):
        r, f, m = row["recency_score"], row["frequency_score"], row["monetary_score"]
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        elif r >= 3 and f >= 3:
            return "Loyal Customers"
        elif r >= 4:
            return "Recent Customers"
        elif f <= 2 and m >= 3:
            return "At Risk Big Spenders"
        else:
            return "Needs Attention"

    spd["customer_segment"] = spd.apply(rfm_segment, axis=1)

    segment_summary = (
        spd.groupby("customer_segment")
        .agg(
            customer_count=("CUSTOMER_ID", "count"),
            avg_lifetime_spend=("LIFETIME_SPEND_USD", "mean"),
            avg_txn_count=("TOTAL_TRANSACTIONS", "mean"),
            fraud_rate=("FRAUD_FLAG_COUNT", lambda x: (x > 0).mean()),
        )
        .reset_index()
        .round(2)
    )

    log.info("Customer segment summary:\n%s", segment_summary.to_string())
    return segment_summary


# ─────────────────────────────────────────────────────────────────────────────
# 4 · Copy ADLS Gold Parquet → Snowflake via External Stage
# ─────────────────────────────────────────────────────────────────────────────
COPY_COMMANDS = """
-- Create external stage pointing to ADLS Gen2
CREATE OR REPLACE STAGE STAGING.ADLS_GOLD_STAGE
    URL           = 'azure://finstream360adls.blob.core.windows.net/datalake/gold/'
    CREDENTIALS   = (AZURE_SAS_TOKEN = '?sv=...')   -- replace with actual SAS
    FILE_FORMAT   = (TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE);

-- Copy daily_txn_summary
COPY INTO GOLD.DAILY_TXN_SUMMARY
FROM @STAGING.ADLS_GOLD_STAGE/daily_txn_summary/
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;

-- Copy customer_360
COPY INTO GOLD.CUSTOMER_360
FROM @STAGING.ADLS_GOLD_STAGE/customer_360/
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR    = 'CONTINUE';
"""


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────
def main():
    session = get_session()

    log.info("── Step 1: Data Quality Checks ──")
    dq_results = run_dq_checks(session)
    if not dq_results["all_passed"]:
        log.warning("DQ checks FAILED — proceeding anyway (flag only)")

    log.info("── Step 2: Wrangle Transactions ──")
    wrangle_transactions(session)

    log.info("── Step 3: Customer Segment Analysis ──")
    segments = compute_customer_segments(session)
    print(segments.to_markdown(index=False))

    session.close()
    log.info("Snowpark ETL complete.")


if __name__ == "__main__":
    main()
