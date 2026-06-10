# Databricks notebook source
# MAGIC %md
# MAGIC # FinStream360 — Silver Layer (Cleanse & Enrich)
# MAGIC
# MAGIC **Purpose:** Transform Bronze raw data into a validated, enriched, deduplicated
# MAGIC Silver Delta table ready for analytics and ML consumption.
# MAGIC
# MAGIC ## Transformations applied
# MAGIC - Schema validation & null handling
# MAGIC - Deduplication on `transaction_id`
# MAGIC - Timestamp parsing to native `TimestampType`
# MAGIC - Amount normalisation (USD only; FX stub included)
# MAGIC - Customer dimension join (SCD-1 snapshot)
# MAGIC - Derived features: `txn_hour`, `txn_day_of_week`, `is_weekend`, `is_cross_state`
# MAGIC - Data quality flag column (`dq_passed`)
# MAGIC
# MAGIC **Author:** Akhil Basavanapalli
# MAGIC **Tech Stack:** Databricks, PySpark, Delta Lake, Spark SQL

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ── Paths
STORAGE_ACCOUNT  = "finstream360adls"
CONTAINER        = "datalake"
BASE_PATH        = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

BRONZE_TXN       = "finstream360_bronze.transactions"
BRONZE_CUSTOMER  = "finstream360_bronze.customers"

SILVER_TXN       = f"{BASE_PATH}/silver/transactions"
SILVER_CUSTOMER  = f"{BASE_PATH}/silver/customers"
SILVER_TXN_ENRCH = f"{BASE_PATH}/silver/transactions_enriched"

# COMMAND ----------

# MAGIC %md ## 1 · Read Bronze

# COMMAND ----------

bronze_txn  = spark.table(BRONZE_TXN)
bronze_cust = spark.table(BRONZE_CUSTOMER)

print(f"Bronze transactions : {bronze_txn.count():,}")
print(f"Bronze customers    : {bronze_cust.count():,}")

# COMMAND ----------

# MAGIC %md ## 2 · Cleanse Transactions

# COMMAND ----------

def cleanse_transactions(df):
    """
    Apply data quality rules and produce a DQ flag.
    Records failing DQ are NOT dropped — they are flagged for downstream triage.
    """
    return (
        df
        # ── Parse timestamps
        .withColumn("transaction_ts",   F.to_timestamp("transaction_ts"))
        .withColumn("bronze_ingested_at", F.to_timestamp("bronze_ingested_at"))

        # ── Normalise strings
        .withColumn("merchant_category", F.upper(F.trim("merchant_category")))
        .withColumn("merchant_state",    F.upper(F.trim("merchant_state")))
        .withColumn("card_type",         F.upper(F.trim("card_type")))
        .withColumn("currency",          F.upper(F.trim("currency")))
        .withColumn("response_code",     F.trim("response_code"))

        # ── Amount sanity
        .withColumn("amount_usd_clean",
                    F.when(F.col("amount_usd") <= 0, None).otherwise(F.col("amount_usd")))

        # ── Data quality flag
        .withColumn("dq_passed",
                    F.when(
                        F.col("transaction_id").isNull()
                        | F.col("customer_id").isNull()
                        | F.col("amount_usd_clean").isNull()
                        | F.col("transaction_ts").isNull(),
                        False
                    ).otherwise(True))

        # ── Drop the uncleaned amount col
        .drop("amount_usd")
        .withColumnRenamed("amount_usd_clean", "amount_usd")
    )

# COMMAND ----------

# MAGIC %md ## 3 · Deduplicate (keep latest kafka_offset per transaction_id)

# COMMAND ----------

def deduplicate_transactions(df):
    """Keep the most recent record per transaction_id (idempotent re-runs)."""
    w = Window.partitionBy("transaction_id").orderBy(F.desc("kafka_offset"))
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )

# COMMAND ----------

# MAGIC %md ## 4 · Derive Features

# COMMAND ----------

def add_derived_features(df):
    """
    Add analytical and ML-ready feature columns.
    """
    return (
        df
        .withColumn("txn_year",        F.year("transaction_ts"))
        .withColumn("txn_month",       F.month("transaction_ts"))
        .withColumn("txn_day",         F.dayofmonth("transaction_ts"))
        .withColumn("txn_hour",        F.hour("transaction_ts"))
        .withColumn("txn_day_of_week", F.dayofweek("transaction_ts"))   # 1=Sun, 7=Sat
        .withColumn("is_weekend",      F.col("txn_day_of_week").isin(1, 7))
        .withColumn("is_late_night",   F.col("txn_hour").between(0, 5))
        .withColumn("txn_quarter",     F.quarter("transaction_ts"))
        .withColumn("silver_processed_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md ## 5 · Cleanse Customers

# COMMAND ----------

def cleanse_customers(df):
    return (
        df
        .withColumn("account_open_date", F.to_date("account_open_date"))
        .withColumn("created_at",        F.to_timestamp("created_at"))
        .withColumn("email",             F.lower(F.trim("email")))
        .withColumn("home_state",        F.upper(F.trim("home_state")))
        .withColumn("card_type",         F.upper(F.trim("card_type")))
        .withColumn("account_age_days",
                    F.datediff(F.current_date(), F.col("account_open_date")))
        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------

# MAGIC %md ## 6 · Enrich Transactions with Customer Dimension

# COMMAND ----------

def enrich_transactions(txn_df, cust_df):
    """
    Join transactions to customer dimension to add credit profile context.
    Only non-PII columns are carried forward for analytics.
    """
    cust_slim = cust_df.select(
        "customer_id", "home_state", "credit_score",
        "credit_limit_usd", "account_age_days", "card_type"
    ).withColumnRenamed("card_type", "cust_card_type")

    enriched = txn_df.join(cust_slim, on="customer_id", how="left")

    return (
        enriched
        .withColumn("is_cross_state",
                    F.col("merchant_state") != F.col("home_state"))
        .withColumn("amount_to_limit_ratio",
                    F.round(F.col("amount_usd") / F.col("credit_limit_usd"), 4))
    )

# COMMAND ----------

# MAGIC %md ## 7 · Run the pipeline

# COMMAND ----------

silver_txn  = (
    bronze_txn
    .transform(cleanse_transactions)
    .transform(deduplicate_transactions)
    .transform(add_derived_features)
)

silver_cust = bronze_cust.transform(cleanse_customers)
silver_enrch = enrich_transactions(silver_txn, silver_cust)

print(f"Silver transactions (enriched) : {silver_enrch.count():,}")
dq_fail = silver_enrch.filter("dq_passed = false").count()
print(f"DQ failures                    : {dq_fail:,}")

# COMMAND ----------

# MAGIC %md ## 8 · Write to Silver Delta (MERGE / Upsert for idempotency)

# COMMAND ----------

def upsert_to_delta(source_df, target_path: str, merge_key: str):
    """
    MERGE source into the target Delta table using merge_key.
    Inserts new rows; updates existing ones.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("tgt")
                  .merge(source_df.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
                  .whenMatchedUpdateAll()
                  .whenNotMatchedInsertAll()
                  .execute()
        )
        print(f"MERGE complete → {target_path}")
    else:
        (
            source_df.write
                     .format("delta")
                     .mode("overwrite")
                     .partitionBy("txn_year", "txn_month")
                     .save(target_path)
        )
        print(f"Initial write complete → {target_path}")


upsert_to_delta(silver_enrch, SILVER_TXN_ENRCH, "transaction_id")
upsert_to_delta(silver_cust,  SILVER_CUSTOMER,  "customer_id")

# COMMAND ----------

# MAGIC %md ## 9 · Register Silver tables in Unity Catalog

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS finstream360_silver")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS finstream360_silver.transactions_enriched
    USING DELTA LOCATION '{SILVER_TXN_ENRCH}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS finstream360_silver.customers
    USING DELTA LOCATION '{SILVER_CUSTOMER}'
""")

# COMMAND ----------

# MAGIC %md ## 10 · Quality Report

# COMMAND ----------

display(
    silver_enrch
    .groupBy("dq_passed", "merchant_category")
    .agg(
        F.count("*").alias("row_count"),
        F.avg("amount_usd").alias("avg_amount"),
        F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
    )
    .orderBy("dq_passed", F.desc("row_count"))
)
