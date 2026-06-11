# Databricks notebook source
# MAGIC %md
# MAGIC # FinStream360 — Gold Layer (Business Aggregations)
# MAGIC
# MAGIC **Purpose:** Produce business-ready aggregated tables consumed directly by
# MAGIC Power BI dashboards, Snowflake reporting marts, and executive KPI views.
# MAGIC
# MAGIC ## Gold tables produced
# MAGIC | Table | Grain | Consumers |
# MAGIC |-------|-------|-----------|
# MAGIC | `daily_txn_summary`       | Merchant category × Day        | Finance, Power BI |
# MAGIC | `customer_360`            | Customer × All-time            | CRM, Risk         |
# MAGIC | `fraud_alerts_hourly`     | Hour × Merchant Category       | Risk, Ops         |
# MAGIC | `state_performance`       | State × Month                  | Strategy          |
# MAGIC | `card_type_analysis`      | Card Type × Month              | Product           |
# MAGIC
# MAGIC **Author:** Akhil Basavanapalli
# MAGIC **Tech Stack:** Databricks, PySpark, Spark SQL, Delta Lake, Power BI

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

STORAGE_ACCOUNT = "finstream360adls"
CONTAINER = "datalake"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH = f"{BASE_PATH}/gold"

silver_txn = spark.table("finstream360_silver.transactions_enriched")
silver_cust = spark.table("finstream360_silver.customers")

# COMMAND ----------

# MAGIC %md ## 1 · Daily Transaction Summary (KPI fact table)

# COMMAND ----------

daily_txn_summary = (
    silver_txn.filter("dq_passed = true")
    .groupBy("txn_year", "txn_month", "txn_day", "merchant_category", "card_type")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount_usd").alias("total_amount_usd"),
        F.avg("amount_usd").alias("avg_amount_usd"),
        F.max("amount_usd").alias("max_amount_usd"),
        F.min("amount_usd").alias("min_amount_usd"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
        F.sum(F.when(F.col("is_fraud"), F.col("amount_usd")).otherwise(0)).alias("fraud_amount_usd"),
        F.sum(F.col("is_weekend").cast("int")).alias("weekend_txn_count"),
        F.sum(F.col("is_late_night").cast("int")).alias("late_night_txn_count"),
    )
    .withColumn("fraud_rate_pct", F.round(F.col("fraud_count") / F.col("total_transactions") * 100, 2))
    .withColumn("gold_created_at", F.current_timestamp())
    .withColumn("txn_date", F.to_date(F.concat_ws("-", "txn_year", "txn_month", "txn_day")))
)

(
    daily_txn_summary.write.format("delta")
    .mode("overwrite")
    .partitionBy("txn_year", "txn_month")
    .save(f"{GOLD_PATH}/daily_txn_summary")
)
print(f"daily_txn_summary: {daily_txn_summary.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 2 · Customer 360 (lifetime value + risk profile)

# COMMAND ----------

customer_txn_stats = (
    silver_txn.filter("dq_passed = true")
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount_usd").alias("lifetime_spend_usd"),
        F.avg("amount_usd").alias("avg_txn_usd"),
        F.max("amount_usd").alias("max_single_txn_usd"),
        F.countDistinct("merchant_category").alias("unique_categories"),
        F.sum(F.col("is_fraud").cast("int")).alias("fraud_flag_count"),
        F.max("transaction_ts").alias("last_txn_ts"),
        F.min("transaction_ts").alias("first_txn_ts"),
        F.sum(F.col("is_cross_state").cast("int")).alias("cross_state_txn_count"),
        F.avg("amount_to_limit_ratio").alias("avg_amount_to_limit_ratio"),
    )
    .withColumn("days_active", F.datediff(F.col("last_txn_ts"), F.col("first_txn_ts")))
    .withColumn(
        "avg_daily_spend",
        F.when(F.col("days_active") > 0, F.col("lifetime_spend_usd") / F.col("days_active")).otherwise(
            F.col("lifetime_spend_usd")
        ),
    )
    .withColumn(
        "risk_tier",
        F.when(F.col("fraud_flag_count") >= 3, "HIGH").when(F.col("fraud_flag_count") >= 1, "MEDIUM").otherwise("LOW"),
    )
)

customer_360 = silver_cust.join(customer_txn_stats, on="customer_id", how="left").withColumn(
    "gold_created_at", F.current_timestamp()
)

(customer_360.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/customer_360"))
print(f"customer_360: {customer_360.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 3 · Fraud Alerts — Hourly Aggregation

# COMMAND ----------

fraud_alerts_hourly = (
    silver_txn.filter("is_fraud = true")
    .groupBy("txn_year", "txn_month", "txn_day", "txn_hour", "merchant_category", "merchant_state")
    .agg(
        F.count("*").alias("fraud_count"),
        F.sum("amount_usd").alias("fraud_amount_usd"),
        F.countDistinct("customer_id").alias("affected_customers"),
        F.avg("amount_usd").alias("avg_fraud_amount"),
    )
    .withColumn(
        "alert_ts",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.concat_ws("-", "txn_year", "txn_month", "txn_day"),
                F.concat(F.col("txn_hour").cast("string"), F.lit(":00:00")),
            )
        ),
    )
    .withColumn(
        "severity",
        F.when(F.col("fraud_count") >= 50, "CRITICAL")
        .when(F.col("fraud_count") >= 20, "HIGH")
        .when(F.col("fraud_count") >= 5, "MEDIUM")
        .otherwise("LOW"),
    )
    .withColumn("gold_created_at", F.current_timestamp())
)

(
    fraud_alerts_hourly.write.format("delta")
    .mode("overwrite")
    .partitionBy("txn_year", "txn_month")
    .save(f"{GOLD_PATH}/fraud_alerts_hourly")
)
print(f"fraud_alerts_hourly: {fraud_alerts_hourly.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 4 · State Performance (Geographic Analysis)

# COMMAND ----------

state_performance = (
    silver_txn.filter("dq_passed = true")
    .groupBy("txn_year", "txn_month", "merchant_state")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount_usd").alias("total_volume_usd"),
        F.avg("amount_usd").alias("avg_txn_usd"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
    )
    .withColumn("fraud_rate_pct", F.round(F.col("fraud_count") / F.col("total_transactions") * 100, 2))
    .withColumn(
        "txn_month_label", F.concat(F.col("txn_year"), F.lit("-"), F.lpad(F.col("txn_month").cast("string"), 2, "0"))
    )
    .withColumn("gold_created_at", F.current_timestamp())
)

(
    state_performance.write.format("delta")
    .mode("overwrite")
    .partitionBy("txn_year", "txn_month")
    .save(f"{GOLD_PATH}/state_performance")
)
print(f"state_performance: {state_performance.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 5 · Register Gold tables in Metastore

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS finstream360_gold")

gold_tables = {
    "daily_txn_summary": f"{GOLD_PATH}/daily_txn_summary",
    "customer_360": f"{GOLD_PATH}/customer_360",
    "fraud_alerts_hourly": f"{GOLD_PATH}/fraud_alerts_hourly",
    "state_performance": f"{GOLD_PATH}/state_performance",
}

for table_name, path in gold_tables.items():
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS finstream360_gold.{table_name}
        USING DELTA LOCATION '{path}'
    """
    )
    print(f"Registered: finstream360_gold.{table_name}")

# COMMAND ----------

# MAGIC %md ## 6 · Executive KPI snapshot (Spark SQL)

# COMMAND ----------

spark.sql(
    """
    SELECT
        txn_year,
        txn_month,
        SUM(total_transactions)  AS total_txns,
        SUM(total_amount_usd)    AS total_volume_usd,
        SUM(fraud_count)         AS total_fraud_txns,
        ROUND(SUM(fraud_amount_usd), 2) AS total_fraud_amount,
        ROUND(AVG(fraud_rate_pct), 2)   AS avg_fraud_rate_pct,
        COUNT(DISTINCT merchant_category) AS active_categories
    FROM finstream360_gold.daily_txn_summary
    GROUP BY txn_year, txn_month
    ORDER BY txn_year, txn_month
"""
).show(truncate=False)
