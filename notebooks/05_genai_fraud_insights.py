# Databricks notebook source
# MAGIC %md
# MAGIC # FinStream360 — GenAI Layer: AI-Powered Fraud Investigation Reports
# MAGIC
# MAGIC **Purpose:** Uses **Azure OpenAI GPT-4o** to read aggregated fraud data from the
# MAGIC Gold Delta layer and generate:
# MAGIC
# MAGIC 1. **Executive Fraud Summary** — plain-English daily briefing for risk leadership
# MAGIC 2. **Anomaly Narration** — GPT-4o explains *why* a transaction cluster looks suspicious
# MAGIC 3. **Recommended Actions** — AI-generated remediation steps per merchant category
# MAGIC 4. **Semantic Search over Fraud Alerts** — vector embeddings + cosine similarity
# MAGIC    so risk analysts can ask natural language questions like
# MAGIC    *"Show me late-night cross-state travel fraud last week"*
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Gold Delta Tables
# MAGIC       │
# MAGIC       ▼
# MAGIC  Context Builder (PySpark) ──► Azure OpenAI GPT-4o ──► Narrative Report
# MAGIC       │                                                        │
# MAGIC       ▼                                                        ▼
# MAGIC  text-embedding-ada-002 ──► Chroma Vector Store ──► Semantic Search API
# MAGIC ```
# MAGIC
# MAGIC **Author:** Akhil Basavanapalli
# MAGIC **Tech Stack:** Azure OpenAI, GPT-4o, text-embedding-ada-002, LangChain,
# MAGIC               Chroma DB, Databricks, PySpark, Delta Lake

# COMMAND ----------

# MAGIC %md ## 0 · Install & Import

# COMMAND ----------

# MAGIC %pip install openai==1.30.1 langchain==0.2.1 langchain-openai==0.1.8 \
# MAGIC              chromadb==0.5.0 tiktoken==0.7.0 --quiet

# COMMAND ----------

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import functions as F

from openai import AzureOpenAI
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain.schema import HumanMessage, SystemMessage
from langchain.text_splitter import RecursiveCharacterTextSplitter
import chromadb

logger = logging.getLogger("genai_fraud_insights")

# ── Azure OpenAI credentials (from Databricks Secrets / Key Vault) ───────────
AZURE_OAI_ENDPOINT = dbutils.secrets.get("finstream360", "azure_oai_endpoint")  # noqa
AZURE_OAI_KEY = dbutils.secrets.get("finstream360", "azure_oai_key")  # noqa
AZURE_OAI_DEPLOYMENT = "gpt-4o"  # your GPT-4o deployment name
AZURE_EMB_DEPLOYMENT = "text-embedding-ada-002"  # embedding model deployment

# ── LangChain clients ─────────────────────────────────────────────────────────
llm = AzureChatOpenAI(
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_key=AZURE_OAI_KEY,
    azure_deployment=AZURE_OAI_DEPLOYMENT,
    api_version="2024-02-01",
    temperature=0.2,  # low temp for factual financial reports
    max_tokens=2048,
)

embeddings = AzureOpenAIEmbeddings(
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_key=AZURE_OAI_KEY,
    azure_deployment=AZURE_EMB_DEPLOYMENT,
    api_version="2024-02-01",
)

print("Azure OpenAI clients initialised ✓")

# COMMAND ----------

# MAGIC %md ## 1 · Pull Gold Data as Context

# COMMAND ----------


def get_fraud_context(days_back: int = 1) -> dict:
    """
    Reads the Gold layer and builds a structured context dict
    that will be serialised to JSON and passed to GPT-4o.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # Daily fraud summary
    daily = (
        spark.table("finstream360_gold.daily_txn_summary")
        .filter(F.col("txn_date") >= cutoff)
        .orderBy(F.desc("fraud_count"))
        .limit(20)
        .toPandas()
    )

    # Hourly alerts (severity HIGH/CRITICAL only)
    alerts = (
        spark.table("finstream360_gold.fraud_alerts_hourly")
        .filter((F.col("txn_year") == datetime.utcnow().year) & (F.col("severity").isin("HIGH", "CRITICAL")))
        .orderBy(F.desc("fraud_amount_usd"))
        .limit(10)
        .toPandas()
    )

    # Top 5 high-risk customers
    risky_customers = (
        spark.table("finstream360_gold.customer_360")
        .filter(F.col("risk_tier") == "HIGH")
        .orderBy(F.desc("fraud_flag_count"))
        .select("customer_id", "home_state", "credit_score", "fraud_flag_count", "lifetime_spend_usd", "risk_tier")
        .limit(5)
        .toPandas()
    )

    # ML model scores — VERY_HIGH band
    high_score = (
        spark.table("finstream360_gold.fraud_predictions")
        .filter(F.col("fraud_score_band") == "VERY_HIGH")
        .agg(
            F.count("*").alias("very_high_count"),
            F.avg("fraud_probability").alias("avg_prob"),
        )
        .toPandas()
    )

    return {
        "report_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "daily_summary": daily.to_dict(orient="records"),
        "high_severity_alerts": alerts.to_dict(orient="records"),
        "high_risk_customers": risky_customers.to_dict(orient="records"),
        "ml_very_high_scores": high_score.to_dict(orient="records"),
    }


context = get_fraud_context(days_back=1)
print(f"Context built — {len(context['daily_summary'])} daily rows, " f"{len(context['high_severity_alerts'])} alerts")

# COMMAND ----------

# MAGIC %md ## 2 · Generate Executive Fraud Briefing with GPT-4o

# COMMAND ----------

SYSTEM_PROMPT = """You are a senior fraud risk analyst at a financial services company.
You receive structured transaction data from a real-time analytics platform and produce
concise, accurate, and actionable fraud intelligence briefings.

Rules:
- Be specific: cite numbers, percentages, merchant categories, states
- Flag the top 3 risks clearly
- Recommend concrete next steps for the risk operations team
- Format output as a professional briefing (not bullet points — use paragraphs with headers)
- Never speculate beyond what the data shows
- Tone: professional, direct, data-driven
"""

USER_PROMPT = f"""
Analyse the following fraud data from FinStream360 and produce:

1. **Executive Summary** (2–3 sentences, key headline numbers)
2. **Top Risk Signals** (3 specific findings with data evidence)
3. **Merchant Category Analysis** (which categories are most impacted and why)
4. **ML Model Highlights** (what the GBT fraud detection model is flagging)
5. **Recommended Immediate Actions** (3 concrete steps for the risk ops team)

DATA:
{json.dumps(context, indent=2, default=str)}

Today's date: {context['report_date']}
"""

messages = [
    SystemMessage(content=SYSTEM_PROMPT),
    HumanMessage(content=USER_PROMPT),
]

print("Calling GPT-4o for executive fraud briefing...")
response = llm.invoke(messages)
fraud_report = response.content

print("\n" + "=" * 70)
print("FINSTREAM360 — AI FRAUD INTELLIGENCE BRIEFING")
print("=" * 70)
print(fraud_report)
print("=" * 70)

# COMMAND ----------

# MAGIC %md ## 3 · Anomaly Narration — Explain Individual Fraud Clusters

# COMMAND ----------


def explain_fraud_cluster(cluster_data: dict) -> str:
    """
    Ask GPT-4o to explain why a specific transaction cluster is suspicious
    and what pattern it matches.
    """
    prompt = f"""
    A fraud detection model flagged the following transaction cluster as HIGH risk.
    Explain in 3–4 sentences:
    (a) What fraud pattern this most likely represents
    (b) Why these specific features make it suspicious
    (c) What additional data a fraud analyst should check

    Transaction cluster data:
    {json.dumps(cluster_data, indent=2, default=str)}
    """

    resp = llm.invoke(
        [
            SystemMessage(content="You are an expert fraud investigator. Be specific and technical."),
            HumanMessage(content=prompt),
        ]
    )
    return resp.content


# Run for top 3 high-severity alerts
print("\n── ANOMALY NARRATIONS ──────────────────────────────────────────────\n")
for i, alert in enumerate(context["high_severity_alerts"][:3], 1):
    print(
        f"Alert #{i}: {alert.get('merchant_category')} | "
        f"{alert.get('merchant_state')} | "
        f"${alert.get('fraud_amount_usd', 0):,.2f}"
    )
    explanation = explain_fraud_cluster(alert)
    print(f"AI Analysis: {explanation}\n")
    print("-" * 60)

# COMMAND ----------

# MAGIC %md ## 4 · Semantic Search over Fraud Alerts (Vector Embeddings)

# COMMAND ----------


def build_vector_store(session) -> chromadb.Collection:
    """
    Embeds each Gold fraud alert row using text-embedding-ada-002
    and stores in a local Chroma collection for semantic search.
    """
    alerts_df = (
        session.table("finstream360_gold.fraud_alerts_hourly")
        .filter(F.col("fraud_count") > 0)
        .orderBy(F.desc("fraud_amount_usd"))
        .limit(500)
        .toPandas()
    )

    # Build natural-language description per alert row
    docs, doc_ids, metadatas = [], [], []
    for _, row in alerts_df.iterrows():
        text = (
            f"On {row['txn_year']}-{row['txn_month']:02d}-{row['txn_day']:02d} "
            f"at hour {row['txn_hour']}, "
            f"{row['fraud_count']} fraudulent {row['merchant_category']} transactions "
            f"occurred in {row['merchant_state']} totalling "
            f"${row['fraud_amount_usd']:,.2f}, affecting "
            f"{row['affected_customers']} customers. "
            f"Severity: {row['severity']}."
        )
        docs.append(text)
        doc_ids.append(str(row.get("alert_id", _)))
        metadatas.append(
            {
                "category": str(row["merchant_category"]),
                "state": str(row["merchant_state"]),
                "severity": str(row["severity"]),
                "month": int(row["txn_month"]),
                "year": int(row["txn_year"]),
            }
        )

    # Embed with Azure OpenAI text-embedding-ada-002
    print(f"Embedding {len(docs)} fraud alert records...")
    vectors = embeddings.embed_documents(docs)

    # Store in Chroma
    client = chromadb.Client()
    collection = client.get_or_create_collection("finstream360_fraud_alerts")
    collection.upsert(
        ids=doc_ids,
        embeddings=vectors,
        documents=docs,
        metadatas=metadatas,
    )

    print(f"Vector store ready — {collection.count()} documents indexed ✓")
    return collection


fraud_collection = build_vector_store(spark)

# COMMAND ----------


def semantic_search(collection, query: str, n_results: int = 5) -> list:
    """
    Embed the query and find the most similar fraud alerts.
    """
    query_vector = embeddings.embed_query(query)
    results = collection.query(
        query_embeddings=[query_vector],
        n_results=n_results,
    )

    hits = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    ):
        hits.append(
            {
                "similarity": round(1 - dist, 4),
                "text": doc,
                "metadata": meta,
            }
        )
    return hits


# ── Example semantic queries ──────────────────────────────────────────────────
queries = [
    "late night cross-state travel fraud",
    "high value online retail transactions flagged in Texas",
    "ATM withdrawal anomalies critical severity",
]

print("\n── SEMANTIC SEARCH RESULTS ─────────────────────────────────────────\n")
for q in queries:
    print(f'Query: "{q}"')
    hits = semantic_search(fraud_collection, q, n_results=3)
    for hit in hits:
        print(f"  [{hit['similarity']:.3f}] {hit['text'][:120]}...")
    print()

# COMMAND ----------

# MAGIC %md ## 5 · RAG — Answer Risk Analyst Questions from Gold Data

# COMMAND ----------


def rag_answer(collection, question: str) -> str:
    """
    Retrieval-Augmented Generation:
    1. Retrieve top-k relevant fraud alert docs from Chroma
    2. Pass them as context to GPT-4o with the analyst's question
    3. Return a grounded, cited answer
    """
    hits = semantic_search(collection, question, n_results=8)
    context_ = "\n\n".join([f"[Alert {i+1}]: {h['text']}" for i, h in enumerate(hits)])

    prompt = f"""
    You are a fraud risk analyst assistant for FinStream360.
    Answer the analyst's question using ONLY the retrieved fraud alert data below.
    Cite specific alerts by number. If the data doesn't support an answer, say so.

    RETRIEVED ALERTS:
    {context_}

    ANALYST QUESTION: {question}
    """

    resp = llm.invoke(
        [
            SystemMessage(content="Answer concisely and cite the alert numbers you used."),
            HumanMessage(content=prompt),
        ]
    )
    return resp.content


# ── Demo RAG queries ──────────────────────────────────────────────────────────
rag_questions = [
    "Which merchant category had the highest fraud amount last month?",
    "Are there any patterns in states where critical fraud alerts occur?",
    "What time of day sees the most severe fraud activity?",
]

print("\n── RAG: ANALYST Q&A ────────────────────────────────────────────────\n")
for question in rag_questions:
    print(f"Q: {question}")
    answer = rag_answer(fraud_collection, question)
    print(f"A: {answer}\n")
    print("-" * 60)

# COMMAND ----------

# MAGIC %md ## 6 · Save AI Report to Gold Layer

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

report_schema = StructType(
    [
        StructField("report_id", StringType(), False),
        StructField("report_type", StringType(), True),
        StructField("report_content", StringType(), True),
        StructField("model_used", StringType(), True),
        StructField("generated_at", TimestampType(), True),
    ]
)

import uuid

report_df = spark.createDataFrame(
    [
        (
            str(uuid.uuid4()),
            "DAILY_FRAUD_BRIEFING",
            fraud_report,
            AZURE_OAI_DEPLOYMENT,
            datetime.utcnow(),
        )
    ],
    schema=report_schema,
)

STORAGE_ACCOUNT = "finstream360adls"
CONTAINER = "datalake"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

(report_df.write.format("delta").mode("append").save(f"{BASE_PATH}/gold/ai_reports"))

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS finstream360_gold.ai_reports
    USING DELTA LOCATION '{BASE_PATH}/gold/ai_reports'
"""
)

print("AI report saved to Gold layer → finstream360_gold.ai_reports ✓")
print(f"Report ID: {report_df.collect()[0]['report_id']}")
