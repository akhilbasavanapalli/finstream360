# Databricks notebook source
# MAGIC %md
# MAGIC # FinStream360 — AI Data Quality Assistant
# MAGIC
# MAGIC **Purpose:** Uses **Azure OpenAI GPT-4o** as an intelligent DQ co-pilot that:
# MAGIC
# MAGIC - Monitors pipeline health across Bronze/Silver/Gold layers
# MAGIC - Explains anomalies in plain English (no SQL knowledge needed)
# MAGIC - Suggests root causes and fixes for data quality failures
# MAGIC - Generates natural language DQ reports for stakeholders
# MAGIC - Answers ad-hoc questions about data freshness and completeness
# MAGIC
# MAGIC **Author:** Akhil Basavanapalli
# MAGIC **Tech Stack:** Azure OpenAI GPT-4o, LangChain Agents, PySpark, Delta Lake

# COMMAND ----------

# MAGIC %pip install openai langchain langchain-openai --quiet

# COMMAND ----------

import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from langchain_openai import AzureChatOpenAI
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.tools import tool
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.schema import SystemMessage

# Azure OpenAI setup
AZURE_OAI_ENDPOINT   = dbutils.secrets.get("finstream360", "azure_oai_endpoint")  # noqa
AZURE_OAI_KEY        = dbutils.secrets.get("finstream360", "azure_oai_key")       # noqa

llm = AzureChatOpenAI(
    azure_endpoint   = AZURE_OAI_ENDPOINT,
    api_key          = AZURE_OAI_KEY,
    azure_deployment = "gpt-4o",
    api_version      = "2024-02-01",
    temperature      = 0.1,
)

# COMMAND ----------

# MAGIC %md ## 1 · DQ Metrics Collector

# COMMAND ----------

def collect_dq_metrics() -> dict:
    """Collect DQ metrics across all three medallion layers."""

    bronze = spark.table("finstream360_bronze.transactions")
    silver = spark.table("finstream360_silver.transactions_enriched")
    gold   = spark.table("finstream360_gold.daily_txn_summary")

    bronze_total = bronze.count()
    silver_total = silver.count()
    dq_fail      = silver.filter("dq_passed = false").count()

    # Freshness check — when was the last record loaded?
    bronze_latest = bronze.agg(F.max("bronze_ingested_at")).collect()[0][0]
    silver_latest = silver.agg(F.max("silver_processed_at")).collect()[0][0]
    gold_latest   = gold.agg(F.max("gold_created_at")).collect()[0][0]

    # Null rates in Silver
    null_rates = {}
    key_cols = ["transaction_id", "customer_id", "amount_usd", "transaction_ts", "merchant_category"]
    for col in key_cols:
        null_count = silver.filter(F.col(col).isNull()).count()
        null_rates[col] = round(null_count / silver_total * 100, 4) if silver_total > 0 else 0

    # Duplicate check
    dup_count = silver_total - silver.select("transaction_id").distinct().count()

    # Amount anomaly — flag unusually high single transactions
    p99_amount = silver.approxQuantile("amount_usd", [0.99], 0.01)[0]
    high_amount_count = silver.filter(F.col("amount_usd") > p99_amount * 3).count()

    # Category distribution
    category_dist = (
        silver
        .groupBy("merchant_category")
        .count()
        .orderBy(F.desc("count"))
        .limit(5)
        .toPandas()
        .to_dict(orient="records")
    )

    return {
        "timestamp":          datetime.utcnow().isoformat(),
        "bronze_total_rows":  bronze_total,
        "silver_total_rows":  silver_total,
        "gold_total_rows":    gold.count(),
        "dq_fail_count":      dq_fail,
        "dq_fail_rate_pct":   round(dq_fail / silver_total * 100, 3) if silver_total > 0 else 0,
        "duplicate_count":    dup_count,
        "null_rates":         null_rates,
        "high_amount_anomalies": high_amount_count,
        "p99_amount_usd":     round(p99_amount, 2),
        "bronze_last_loaded": str(bronze_latest),
        "silver_last_loaded": str(silver_latest),
        "gold_last_loaded":   str(gold_latest),
        "top_categories":     category_dist,
        "bronze_to_silver_drop_pct": round(
            (bronze_total - silver_total) / bronze_total * 100, 3
        ) if bronze_total > 0 else 0,
    }


dq_metrics = collect_dq_metrics()
print("DQ metrics collected:")
print(json.dumps(dq_metrics, indent=2, default=str))

# COMMAND ----------

# MAGIC %md ## 2 · LangChain Agent with DQ Tools

# COMMAND ----------

@tool
def get_pipeline_health() -> str:
    """Returns current DQ metrics for the FinStream360 pipeline."""
    return json.dumps(collect_dq_metrics(), indent=2, default=str)


@tool
def get_null_analysis(column_name: str) -> str:
    """
    Analyse null values for a specific column in the Silver transactions table.
    Returns null count, rate, and sample bad rows.
    """
    df    = spark.table("finstream360_silver.transactions_enriched")
    total = df.count()
    nulls = df.filter(F.col(column_name).isNull())
    count = nulls.count()

    sample = nulls.select(
        "transaction_id", "customer_id", "merchant_category",
        "transaction_ts", column_name
    ).limit(3).toPandas().to_dict(orient="records")

    return json.dumps({
        "column":      column_name,
        "null_count":  count,
        "null_rate":   f"{round(count/total*100, 3)}%",
        "sample_rows": sample,
    }, default=str)


@tool
def get_freshness_status() -> str:
    """
    Check data freshness across all layers.
    Returns how many minutes ago each layer was last updated.
    """
    now = datetime.utcnow()
    results = {}
    for layer, table in [
        ("bronze", "finstream360_bronze.transactions"),
        ("silver", "finstream360_silver.transactions_enriched"),
        ("gold",   "finstream360_gold.daily_txn_summary"),
    ]:
        ts_col = {"bronze": "bronze_ingested_at",
                  "silver": "silver_processed_at",
                  "gold":   "gold_created_at"}[layer]
        try:
            latest = spark.table(table).agg(F.max(ts_col)).collect()[0][0]
            lag_minutes = round((now - latest).total_seconds() / 60, 1) if latest else None
            results[layer] = {"last_updated": str(latest), "lag_minutes": lag_minutes}
        except Exception as e:
            results[layer] = {"error": str(e)}

    return json.dumps(results, default=str)


@tool
def explain_dq_issue(issue_description: str) -> str:
    """
    Given a data quality issue description, returns a detailed explanation
    of likely root causes and recommended fixes specific to this pipeline.
    """
    root_cause_map = {
        "null transaction_id": "Likely cause: Kafka consumer deserialisation failure or upstream API sent malformed JSON. Check Bronze ingestion logs and Event Hub dead-letter queue.",
        "negative amount":     "Likely cause: Refund/reversal records ingested without preprocessing. Add ADF data flow filter or Silver-layer CASE logic to handle negatives separately.",
        "duplicate records":   "Likely cause: Kafka consumer committed offset before write succeeded (at-least-once delivery). The Silver MERGE with row_number() dedup should handle this — check checkpoint directory.",
        "high null rate":      "Likely cause: Schema drift in upstream Kafka producer. Compare current transaction_schema with Bronze table schema using `spark.table().printSchema()`.",
        "stale gold":          "Likely cause: ADF pipeline trigger missed a schedule or Databricks cluster auto-terminated. Check ADF monitoring and Databricks job run history.",
    }
    lower = issue_description.lower()
    for key, explanation in root_cause_map.items():
        if key in lower:
            return explanation
    return f"Issue '{issue_description}' not in known patterns. Recommend checking ADF pipeline runs, Databricks job logs, and the AUDIT.PIPELINE_RUN_LOG table in Snowflake."


# ── Build the LangChain Agent ─────────────────────────────────────────────────
tools = [get_pipeline_health, get_null_analysis, get_freshness_status, explain_dq_issue]

system_msg = """You are an expert Data Quality AI Assistant for the FinStream360 pipeline.
You have access to tools that can inspect the Bronze, Silver, and Gold Delta layers.

When answering:
- Always call the relevant tool first before answering
- Cite specific numbers from the tool output
- Suggest concrete fixes, not just descriptions of the problem
- Be concise but complete — data engineers need actionable info fast
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", system_msg),
    ("human",  "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

agent          = create_openai_tools_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, max_iterations=5)

print("DQ Agent ready ✓")

# COMMAND ----------

# MAGIC %md ## 3 · Run the DQ Agent

# COMMAND ----------

dq_questions = [
    "What is the current health of the pipeline? Any critical DQ issues?",
    "Are there any freshness concerns across the medallion layers?",
    "What's the null rate for amount_usd and what might be causing it?",
    "Give me a stakeholder-ready DQ summary I can paste into a Slack message.",
]

print("\n── AI DATA QUALITY ASSISTANT ───────────────────────────────────────\n")
for question in dq_questions:
    print(f"\nQuestion: {question}")
    print("-" * 60)
    result = agent_executor.invoke({"input": question})
    print(f"Answer: {result['output']}")
    print()

# COMMAND ----------

# MAGIC %md ## 4 · Generate Stakeholder DQ Report

# COMMAND ----------

def generate_stakeholder_dq_report(metrics: dict) -> str:
    """Use GPT-4o to turn raw DQ metrics into a readable stakeholder report."""
    prompt = f"""
    Generate a brief Data Quality Report for the FinStream360 pipeline.
    Format it as a Slack message (use emoji sparingly, keep it under 200 words).
    Include: overall health status, any failures or warnings, and one action item.

    Raw DQ metrics:
    {json.dumps(metrics, indent=2, default=str)}
    """
    resp = llm.invoke([SystemMessage(content="You write clear, concise data engineering status updates."),
                       {"role": "user", "content": prompt}])
    return resp.content


report = generate_stakeholder_dq_report(dq_metrics)
print("\n── STAKEHOLDER DQ REPORT (ready for Slack/Teams) ───────────────────")
print(report)
