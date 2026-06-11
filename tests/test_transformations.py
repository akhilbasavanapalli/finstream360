"""
FinStream360 — Unit Tests for PySpark Transformations
=====================================================
Tests the Silver-layer transformation logic using a local SparkSession.
Run with:  pytest tests/ -v --tb=short

Author : Akhil Basavanapalli
Tech   : pytest, PySpark, Delta Lake (local)
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, LongType
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for testing."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("finstream360-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


TXN_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("merchant_state", StringType(), True),
        StructField("amount_usd", DoubleType(), True),
        StructField("transaction_ts", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("card_present", BooleanType(), True),
        StructField("kafka_offset", LongType(), True),
    ]
)


def make_txn_df(spark, rows):
    return spark.createDataFrame(rows, schema=TXN_SCHEMA)


@pytest.fixture
def clean_txn_df(spark):
    return make_txn_df(
        spark,
        [
            ("txn-001", "cust-1", "VISA", "GROCERY_STORE", "TX", 45.50, "2024-03-15T10:30:00Z", False, True, 1),
            ("txn-002", "cust-2", "MASTERCARD", "RESTAURANT", "CA", 89.99, "2024-03-15T12:00:00Z", False, True, 2),
            ("txn-003", "cust-3", "AMEX", "TRAVEL", "NY", 1250.0, "2024-03-15T23:45:00Z", True, False, 3),
        ],
    )


@pytest.fixture
def dirty_txn_df(spark):
    return make_txn_df(
        spark,
        [
            (
                None,
                "cust-1",
                "VISA",
                "GROCERY_STORE",
                "TX",
                45.50,
                "2024-03-15T10:30:00Z",
                False,
                True,
                1,
            ),  # null txn_id
            (
                "txn-002",
                None,
                "MASTERCARD",
                "RESTAURANT",
                "CA",
                89.99,
                "2024-03-15T12:00:00Z",
                False,
                True,
                2,
            ),  # null cust_id
            (
                "txn-003",
                "cust-3",
                "AMEX",
                "TRAVEL",
                "NY",
                -99.0,
                "2024-03-15T23:45:00Z",
                True,
                False,
                3,
            ),  # negative amount
            ("txn-004", "cust-4", "DISCOVER", "GAS_STATION", "FL", 55.0, None, False, True, 4),  # null ts
        ],
    )


@pytest.fixture
def dup_txn_df(spark):
    """Same transaction_id appears twice with different kafka_offsets."""
    return make_txn_df(
        spark,
        [
            ("txn-001", "cust-1", "VISA", "GROCERY_STORE", "TX", 45.50, "2024-03-15T10:30:00Z", False, True, 1),
            ("txn-001", "cust-1", "VISA", "GROCERY_STORE", "TX", 45.50, "2024-03-15T10:30:00Z", False, True, 5),  # dup
            ("txn-002", "cust-2", "MC", "RESTAURANT", "CA", 89.99, "2024-03-15T12:00:00Z", False, True, 2),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Import the transformation functions we want to test
# ─────────────────────────────────────────────────────────────────────────────
def cleanse_transactions(df):
    """Copied inline so tests don't depend on Databricks dbutils."""
    return (
        df.withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
        .withColumn("merchant_category", F.upper(F.trim("merchant_category")))
        .withColumn("merchant_state", F.upper(F.trim("merchant_state")))
        .withColumn("card_type", F.upper(F.trim("card_type")))
        .withColumn("amount_usd_clean", F.when(F.col("amount_usd") <= 0, None).otherwise(F.col("amount_usd")))
        .withColumn(
            "dq_passed",
            F.when(
                F.col("transaction_id").isNull()
                | F.col("customer_id").isNull()
                | F.col("amount_usd_clean").isNull()
                | F.col("transaction_ts").isNull(),
                False,
            ).otherwise(True),
        )
        .drop("amount_usd")
        .withColumnRenamed("amount_usd_clean", "amount_usd")
    )


def deduplicate_transactions(df):
    from pyspark.sql.window import Window

    w = Window.partitionBy("transaction_id").orderBy(F.desc("kafka_offset"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_derived_features(df):
    return (
        df.withColumn("txn_hour", F.hour("transaction_ts"))
        .withColumn("txn_day_of_week", F.dayofweek("transaction_ts"))
        .withColumn("is_weekend", F.col("txn_day_of_week").isin(1, 7))
        .withColumn("is_late_night", F.col("txn_hour").between(0, 5))
        .withColumn("txn_year", F.year("transaction_ts"))
        .withColumn("txn_month", F.month("transaction_ts"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────
class TestCleansing:
    def test_dq_flag_all_pass_on_clean_data(self, clean_txn_df):
        result = cleanse_transactions(clean_txn_df)
        failures = result.filter("dq_passed = false").count()
        assert failures == 0, f"Expected 0 DQ failures, got {failures}"

    def test_dq_flag_marks_null_transaction_id(self, dirty_txn_df):
        result = cleanse_transactions(dirty_txn_df)
        # Row with null transaction_id should fail
        null_id_row = result.filter("customer_id = 'cust-1'").collect()[0]
        assert null_id_row["dq_passed"] is False

    def test_dq_flag_marks_null_customer_id(self, dirty_txn_df):
        result = cleanse_transactions(dirty_txn_df)
        null_cust_row = result.filter("transaction_id = 'txn-002'").collect()[0]
        assert null_cust_row["dq_passed"] is False

    def test_dq_flag_marks_negative_amount(self, dirty_txn_df):
        result = cleanse_transactions(dirty_txn_df)
        neg_row = result.filter("transaction_id = 'txn-003'").collect()[0]
        assert neg_row["dq_passed"] is False

    def test_merchant_category_uppercased(self, clean_txn_df):
        result = cleanse_transactions(clean_txn_df)
        cats = [r["merchant_category"] for r in result.collect()]
        assert all(c == c.upper() for c in cats)

    def test_timestamp_parsed(self, clean_txn_df):
        result = cleanse_transactions(clean_txn_df)
        assert dict(result.dtypes)["transaction_ts"] == "timestamp"

    def test_total_row_count_preserved(self, dirty_txn_df):
        """DQ does NOT drop rows — just flags them."""
        result = cleanse_transactions(dirty_txn_df)
        assert result.count() == dirty_txn_df.count()


class TestDeduplication:
    def test_dedup_removes_duplicate(self, dup_txn_df):
        result = deduplicate_transactions(dup_txn_df)
        assert result.count() == 2

    def test_dedup_keeps_highest_offset(self, dup_txn_df):
        result = deduplicate_transactions(dup_txn_df)
        txn001 = result.filter("transaction_id = 'txn-001'").collect()[0]
        assert txn001["kafka_offset"] == 5  # highest offset kept

    def test_dedup_idempotent(self, clean_txn_df):
        """Running dedup twice should yield same result."""
        once = deduplicate_transactions(clean_txn_df).count()
        twice = deduplicate_transactions(deduplicate_transactions(clean_txn_df)).count()
        assert once == twice


class TestDerivedFeatures:
    def test_late_night_flag_for_midnight(self, clean_txn_df):
        cleansed = cleanse_transactions(clean_txn_df)
        enriched = add_derived_features(cleansed)
        # txn-003 is at 23:45 — NOT late night (late night = 0-5 AM)
        txn003 = enriched.filter("transaction_id = 'txn-003'").collect()[0]
        assert txn003["is_late_night"] is False

    def test_txn_hour_extracted(self, clean_txn_df):
        cleansed = cleanse_transactions(clean_txn_df)
        enriched = add_derived_features(cleansed)
        txn001 = enriched.filter("transaction_id = 'txn-001'").collect()[0]
        assert txn001["txn_hour"] == 10

    def test_year_month_extracted(self, clean_txn_df):
        cleansed = cleanse_transactions(clean_txn_df)
        enriched = add_derived_features(cleansed)
        row = enriched.filter("transaction_id = 'txn-001'").collect()[0]
        assert row["txn_year"] == 2024
        assert row["txn_month"] == 3


class TestDataQualityCounts:
    def test_fraud_count(self, clean_txn_df):
        fraud_count = clean_txn_df.filter("is_fraud = true").count()
        assert fraud_count == 1

    def test_no_negative_amounts_after_cleanse(self, dirty_txn_df):
        result = cleanse_transactions(dirty_txn_df)
        neg_count = result.filter("amount_usd <= 0").count()
        # Null is acceptable; negative is not
        assert neg_count == 0

    def test_amount_null_for_negatives(self, dirty_txn_df):
        result = cleanse_transactions(dirty_txn_df)
        txn003 = result.filter("transaction_id = 'txn-003'").collect()[0]
        assert txn003["amount_usd"] is None
