# Databricks notebook source
# MAGIC %md
# MAGIC # FinStream360 — Bronze Layer (Raw Ingestion)
# MAGIC
# MAGIC **Purpose:** Land raw data from Kafka / Azure Event Hub and batch CSV/Parquet
# MAGIC seeds into the Bronze Delta Lake zone with **zero transformation** — just
# MAGIC schema enforcement, metadata stamping, and partitioned writes.
# MAGIC
# MAGIC | Layer  | Zone       | Format | Partitioning |
# MAGIC |--------|-----------|--------|--------------|
# MAGIC | Bronze | Raw / Landing | Delta  | year / month / day |
# MAGIC
# MAGIC **Author:** Akhil Basavanapalli
# MAGIC **Tech Stack:** Databricks, PySpark, Delta Lake, Azure Event Hub, ADLS Gen2

# COMMAND ----------

# MAGIC %md ## 0 · Imports & Config

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType, LongType
)
import logging

# ── Databricks-native spark session (already available as `spark`)
# spark = SparkSession.builder.getOrCreate()  # uncomment for local testing

logger = logging.getLogger("bronze_ingestion")

# ── Storage paths (ADLS Gen2 / OneLake)
STORAGE_ACCOUNT = "finstream360adls"
CONTAINER       = "datalake"
BASE_PATH       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

BRONZE_TXN      = f"{BASE_PATH}/bronze/transactions"
BRONZE_CUSTOMER = f"{BASE_PATH}/bronze/customers"
BRONZE_MERCHANT = f"{BASE_PATH}/bronze/merchants"

# ── Event Hub / Kafka connection (via Spark Structured Streaming)
EH_NAMESPACE    = dbutils.secrets.get("finstream360", "eh_namespace")    # noqa
EH_CONN_STR     = dbutils.secrets.get("finstream360", "eh_connection_string")  # noqa
EH_TOPIC        = "raw_transactions"

# COMMAND ----------

# MAGIC %md ## 1 · Define Schemas

# COMMAND ----------

transaction_schema = StructType([
    StructField("transaction_id",    StringType(),    False),
    StructField("customer_id",       StringType(),    False),
    StructField("card_type",         StringType(),    True),
    StructField("card_last4",        StringType(),    True),
    StructField("merchant_name",     StringType(),    True),
    StructField("merchant_category", StringType(),    True),
    StructField("merchant_state",    StringType(),    True),
    StructField("amount_usd",        DoubleType(),    False),
    StructField("currency",          StringType(),    True),
    StructField("transaction_ts",    StringType(),    True),
    StructField("is_fraud",          BooleanType(),   True),
    StructField("fraud_reason",      StringType(),    True),
    StructField("card_present",      BooleanType(),   True),
    StructField("response_code",     StringType(),    True),
    StructField("event_created_at",  StringType(),    True),
])

customer_schema = StructType([
    StructField("customer_id",       StringType(),    False),
    StructField("full_name",         StringType(),    True),
    StructField("email",             StringType(),    True),
    StructField("phone",             StringType(),    True),
    StructField("home_state",        StringType(),    True),
    StructField("credit_score",      LongType(),      True),
    StructField("card_type",         StringType(),    True),
    StructField("card_last4",        StringType(),    True),
    StructField("credit_limit_usd",  DoubleType(),    True),
    StructField("account_open_date", StringType(),    True),
    StructField("is_active",         BooleanType(),   True),
    StructField("created_at",        StringType(),    True),
])

# COMMAND ----------

# MAGIC %md ## 2 · Streaming Ingestion from Azure Event Hub (Kafka-compatible endpoint)

# COMMAND ----------

def read_event_hub_stream():
    """
    Reads the raw transaction stream from Azure Event Hub using the
    Kafka-compatible API.  Returns a streaming DataFrame.
    """
    eh_sasl = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f"required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";"
    )

    return (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers",       f"{EH_NAMESPACE}.servicebus.windows.net:9093")
             .option("kafka.security.protocol",       "SASL_SSL")
             .option("kafka.sasl.mechanism",          "PLAIN")
             .option("kafka.sasl.jaas.config",        eh_sasl)
             .option("subscribe",                     EH_TOPIC)
             .option("startingOffsets",               "latest")
             .option("failOnDataLoss",                "false")
             .option("kafka.request.timeout.ms",      "60000")
             .load()
    )


def parse_transactions(raw_df):
    """
    Deserialise JSON payload, cast to schema, and stamp bronze metadata.
    """
    return (
        raw_df
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), transaction_schema).alias("data"),
        )
        .select(
            "kafka_offset",
            "kafka_partition",
            "kafka_timestamp",
            "data.*",
        )
        # Bronze metadata
        .withColumn("bronze_ingested_at",  F.current_timestamp())
        .withColumn("bronze_source",       F.lit("azure_event_hub"))
        .withColumn("_partition_year",     F.year("kafka_timestamp"))
        .withColumn("_partition_month",    F.month("kafka_timestamp"))
        .withColumn("_partition_day",      F.dayofmonth("kafka_timestamp"))
    )


# COMMAND ----------

# MAGIC %md ### 2a · Write streaming transactions to Bronze Delta table

# COMMAND ----------

raw_stream   = read_event_hub_stream()
parsed_stream = parse_transactions(raw_stream)

(
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/bronze_transactions")
    .partitionBy("_partition_year", "_partition_month", "_partition_day")
    .trigger(processingTime="30 seconds")
    .start(BRONZE_TXN)
)

print(f"Streaming write started → {BRONZE_TXN}")

# COMMAND ----------

# MAGIC %md ## 3 · Batch Ingestion (CSV / Parquet seeds from ADLS landing zone)

# COMMAND ----------

LANDING_PATH = f"{BASE_PATH}/landing"


def ingest_batch_parquet(landing_path: str, schema, bronze_path: str, source_name: str):
    """
    Reads Parquet files from the landing zone (dropped by ADF Copy Activity)
    and appends them to the appropriate Bronze Delta table.
    """
    df = (
        spark.read
             .schema(schema)
             .parquet(landing_path)
             .withColumn("bronze_ingested_at", F.current_timestamp())
             .withColumn("bronze_source",       F.lit(source_name))
             .withColumn("_partition_year",  F.year(F.current_timestamp()))
             .withColumn("_partition_month", F.month(F.current_timestamp()))
             .withColumn("_partition_day",   F.dayofmonth(F.current_timestamp()))
    )

    row_count = df.count()
    print(f"Loaded {row_count:,} rows from {landing_path}")

    (
        df.write
          .format("delta")
          .mode("append")
          .partitionBy("_partition_year", "_partition_month", "_partition_day")
          .save(bronze_path)
    )
    print(f"Written to Bronze → {bronze_path}")
    return row_count


# Ingest reference datasets
ingest_batch_parquet(f"{LANDING_PATH}/customers",   customer_schema, BRONZE_CUSTOMER, "batch_seed")
ingest_batch_parquet(f"{LANDING_PATH}/transactions", transaction_schema, BRONZE_TXN,  "batch_seed")

# COMMAND ----------

# MAGIC %md ## 4 · Register Delta tables in the Hive Metastore / Unity Catalog

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS finstream360_bronze LOCATION '{}'".format(
    f"{BASE_PATH}/bronze"
))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS finstream360_bronze.transactions
    USING DELTA
    LOCATION '{BRONZE_TXN}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS finstream360_bronze.customers
    USING DELTA
    LOCATION '{BRONZE_CUSTOMER}'
""")

print("Metastore registration complete.")

# COMMAND ----------

# MAGIC %md ## 5 · Quick validation

# COMMAND ----------

bronze_txn_df = spark.table("finstream360_bronze.transactions")
print(f"Bronze transactions count : {bronze_txn_df.count():,}")
bronze_txn_df.printSchema()
bronze_txn_df.show(5, truncate=False)

display(
    bronze_txn_df
    .groupBy("_partition_year", "_partition_month")
    .agg(F.count("*").alias("row_count"))
    .orderBy("_partition_year", "_partition_month")
)
