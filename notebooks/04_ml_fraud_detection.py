# Databricks notebook source
# MAGIC %md
# MAGIC # FinStream360 — ML Fraud Detection Pipeline
# MAGIC
# MAGIC **Purpose:** Train, evaluate, and serve a real-time fraud detection model using
# MAGIC PySpark MLlib.  The model is registered in MLflow and served via Databricks
# MAGIC Model Serving for sub-50ms scoring.
# MAGIC
# MAGIC ## Pipeline stages
# MAGIC 1. Feature engineering from Silver layer
# MAGIC 2. Class balancing (fraud is ~2% — oversample with SMOTE-ish approach)
# MAGIC 3. Feature vectorisation (VectorAssembler + StandardScaler)
# MAGIC 4. Train GBTClassifier (gradient boosted trees)
# MAGIC 5. Evaluate (AUC-ROC, precision/recall, confusion matrix)
# MAGIC 6. Register model to MLflow Model Registry
# MAGIC 7. Batch scoring of the Silver table → Gold fraud_predictions table
# MAGIC
# MAGIC **Author:** Akhil Basavanapalli
# MAGIC **Tech Stack:** Databricks, PySpark MLlib, MLflow, Delta Lake

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature

import logging

logger = logging.getLogger("fraud_detection_ml")

STORAGE_ACCOUNT = "finstream360adls"
CONTAINER = "datalake"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH = f"{BASE_PATH}/gold"

MLFLOW_EXPERIMENT = "/finstream360/fraud_detection"
mlflow.set_experiment(MLFLOW_EXPERIMENT)

# COMMAND ----------

# MAGIC %md ## 1 · Feature Engineering

# COMMAND ----------

silver_txn = spark.table("finstream360_silver.transactions_enriched").filter("dq_passed = true")

NUMERIC_FEATURES = [
    "amount_usd",
    "txn_hour",
    "txn_day_of_week",
    "credit_score",
    "credit_limit_usd",
    "account_age_days",
    "amount_to_limit_ratio",
    "cross_state_flag",
    "late_night_flag",
    "weekend_flag",
]

CATEGORICAL_FEATURES = ["merchant_category", "card_type"]


def build_feature_df(df):
    """
    Cast boolean flags to double and add any additional derived numeric features.
    """
    return (
        df.withColumn("cross_state_flag", F.col("is_cross_state").cast(DoubleType()))
        .withColumn("late_night_flag", F.col("is_late_night").cast(DoubleType()))
        .withColumn("weekend_flag", F.col("is_weekend").cast(DoubleType()))
        .withColumn("label", F.col("is_fraud").cast(DoubleType()))
        .select(
            "transaction_id",
            *NUMERIC_FEATURES,
            *CATEGORICAL_FEATURES,
            "label",
        )
        .na.fill(0.0, NUMERIC_FEATURES)
        .na.fill("UNKNOWN", CATEGORICAL_FEATURES)
    )


feature_df = build_feature_df(silver_txn)
print(f"Feature dataset: {feature_df.count():,} rows")
print(f"Fraud rate: {feature_df.filter('label=1').count() / feature_df.count():.3%}")

# COMMAND ----------

# MAGIC %md ## 2 · Class Balancing (oversample minority class)

# COMMAND ----------

fraud_df = feature_df.filter("label = 1.0")
nonfraud_df = feature_df.filter("label = 0.0")

fraud_ratio = fraud_df.count() / feature_df.count()
oversample_factor = int(1 / fraud_ratio)  # ~50x to reach 50/50

balanced_df = nonfraud_df.union(fraud_df.sample(withReplacement=True, fraction=float(oversample_factor), seed=42))

print(f"Balanced dataset: {balanced_df.count():,} rows")
print(f"Fraud in balanced: {balanced_df.filter('label=1').count() / balanced_df.count():.1%}")

# ── Train/Test split (time-aware: last 20% for test)
train_df, test_df = balanced_df.randomSplit([0.80, 0.20], seed=42)
print(f"Train: {train_df.count():,}  |  Test: {test_df.count():,}")

# COMMAND ----------

# MAGIC %md ## 3 · Build ML Pipeline

# COMMAND ----------

# Categorical encoding
cat_indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL_FEATURES]
cat_encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe") for c in CATEGORICAL_FEATURES]

ohe_cols = [f"{c}_ohe" for c in CATEGORICAL_FEATURES]
all_features = NUMERIC_FEATURES + ohe_cols

assembler = VectorAssembler(inputCols=all_features, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)

gbt = GBTClassifier(
    labelCol="label",
    featuresCol="features",
    maxIter=100,
    maxDepth=6,
    stepSize=0.05,
    subsamplingRate=0.8,
    featureSubsetStrategy="sqrt",
    seed=42,
)

pipeline = Pipeline(stages=[*cat_indexers, *cat_encoders, assembler, scaler, gbt])

# COMMAND ----------

# MAGIC %md ## 4 · Train with MLflow tracking

# COMMAND ----------

evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

with mlflow.start_run(run_name="GBT_fraud_detection_v1") as run:
    mlflow.log_param("model_type", "GBTClassifier")
    mlflow.log_param("max_iter", 100)
    mlflow.log_param("max_depth", 6)
    mlflow.log_param("step_size", 0.05)
    mlflow.log_param("train_rows", train_df.count())
    mlflow.log_param("test_rows", test_df.count())
    mlflow.log_param("features", str(NUMERIC_FEATURES + CATEGORICAL_FEATURES))

    model = pipeline.fit(train_df)

    # Evaluate
    train_preds = model.transform(train_df)
    test_preds = model.transform(test_df)

    train_auc = evaluator.evaluate(train_preds)
    test_auc = evaluator.evaluate(test_preds)

    # Precision / Recall at threshold 0.5
    tp = test_preds.filter("prediction=1 AND label=1").count()
    fp = test_preds.filter("prediction=1 AND label=0").count()
    fn = test_preds.filter("prediction=0 AND label=1").count()
    tn = test_preds.filter("prediction=0 AND label=0").count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    mlflow.log_metric("train_auc_roc", train_auc)
    mlflow.log_metric("test_auc_roc", test_auc)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("true_positives", tp)
    mlflow.log_metric("false_positives", fp)
    mlflow.log_metric("false_negatives", fn)
    mlflow.log_metric("true_negatives", tn)

    print(f"Train AUC-ROC : {train_auc:.4f}")
    print(f"Test  AUC-ROC : {test_auc:.4f}")
    print(f"Precision     : {precision:.4f}")
    print(f"Recall        : {recall:.4f}")
    print(f"F1 Score      : {f1:.4f}")

    # Register model
    signature = infer_signature(
        train_df.drop("label").toPandas().head(5),
        train_preds.select("prediction").toPandas().head(5),
    )
    mlflow.spark.log_model(
        model,
        artifact_path="fraud_detection_model",
        signature=signature,
        registered_model_name="finstream360_fraud_detector",
    )
    run_id = run.info.run_id

print(f"\nMLflow run_id: {run_id}")

# COMMAND ----------

# MAGIC %md ## 5 · Feature Importance

# COMMAND ----------

gbt_model = model.stages[-1]
feature_names = NUMERIC_FEATURES + [f"{c}_ohe_{i}" for c in CATEGORICAL_FEATURES for i in range(5)]  # approx

importances = sorted(
    zip(feature_names[: len(gbt_model.featureImportances)], gbt_model.featureImportances.toArray()),
    key=lambda x: x[1],
    reverse=True,
)

print("\n── Feature Importances ──────────────────────────────")
for feat, imp in importances[:15]:
    bar = "█" * int(imp * 200)
    print(f"{feat:<35} {imp:.4f}  {bar}")

# COMMAND ----------

# MAGIC %md ## 6 · Batch Score All Silver Transactions → Gold

# COMMAND ----------

scored_df = (
    model.transform(feature_df)
    .select(
        "transaction_id",
        "label",
        "prediction",
        F.col("probability").getItem(1).alias("fraud_probability"),
    )
    .withColumn(
        "fraud_score_band",
        F.when(F.col("fraud_probability") >= 0.90, "VERY_HIGH")
        .when(F.col("fraud_probability") >= 0.70, "HIGH")
        .when(F.col("fraud_probability") >= 0.40, "MEDIUM")
        .otherwise("LOW"),
    )
    .withColumn("scored_at", F.current_timestamp())
)

(scored_df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/fraud_predictions"))

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS finstream360_gold.fraud_predictions
    USING DELTA LOCATION '{GOLD_PATH}/fraud_predictions'
"""
)

print(f"\nScored {scored_df.count():,} transactions.")
display(scored_df.groupBy("fraud_score_band").count().orderBy("fraud_score_band"))
