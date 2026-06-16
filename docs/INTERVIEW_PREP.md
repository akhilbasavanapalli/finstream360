# FinStream360 — Interview Preparation Guide

> Complete recruiter-ready talking points, elevator pitches, and technical Q&A for every stage of the interview process.

---

## 30-Second Elevator Pitch

> *Use this when someone asks "tell me about your portfolio project" at a networking event, career fair, or recruiter phone screen.*

---

**"I built FinStream360 — a real-time financial fraud detection platform on Azure that processes 50,000 credit card transactions per second. It uses Kafka for streaming, Databricks with PySpark for transformation across a Bronze-Silver-Gold medallion architecture, Snowflake for the data mart, and Azure OpenAI GPT-4o for generating executive fraud briefings automatically. The ML model hits AUC-ROC of 0.97 with 91% fraud recall. Everything is infrastructure-as-code with Terraform and deployed through GitHub Actions CI/CD. It's the kind of stack you'd see at a major financial institution — built entirely end-to-end as a single engineer."**

---

## 2-Minute Project Explanation

> *Use this in phone screens, HR rounds, or when a hiring manager says "walk me through your project."*

---

**"FinStream360 is a production-grade data engineering platform I built to demonstrate end-to-end capabilities on the Azure stack.**

**The business problem it solves is two-fold: first, traditional batch fraud detection runs hours after transactions happen — so fraud keeps occurring undetected. Second, financial data is scattered across siloed systems making it impossible to get a single source of truth. FinStream360 solves both.**

**On the architecture side, transactions flow from a Kafka producer at 50,000 per second into Azure Event Hub, which has a Kafka-compatible endpoint. Databricks picks that up with Structured Streaming into a Bronze Delta Lake table — raw, append-only. From there, a Silver transformation layer cleanses the data, deduplicates using Window functions on Kafka offsets for exactly-once semantics, flags data quality issues, and engineers features like transaction hour, day-of-week, and velocity metrics. Gold aggregations produce five business KPI tables — hourly summaries, merchant performance, customer risk scores, geographic analysis, and card type performance.**

**On top of that, a PySpark MLlib Gradient Boosted Tree model trains on the Gold data and scores 10 million rows in 15 minutes, hitting 0.97 AUC-ROC. MLflow tracks every experiment and manages the model registry. Then — and this is where it gets interesting — Azure OpenAI GPT-4o reads the Gold tables and writes executive fraud briefings automatically, with a LangChain Agent monitoring data quality and generating Slack-ready reports.**

**The Gold data lands in Snowflake through the Spark Connector, where stored procedures and Snowflake Tasks keep the mart fresh, and Snowpark Python computes customer RFM segmentation. Power BI connects via DirectQuery.**

**Everything is Terraform IaC — eight Azure resources provisioned in 12 minutes — and GitHub Actions runs lint, unit tests, Terraform validate, and SQL lint on every push."**

---

## 3-Minute Architecture Walkthrough

> *Use this when a hiring manager or technical interviewer says "walk me through the architecture."*

---

**"Let me take you through it layer by layer.**

**Starting with ingestion — I have two paths. The real-time path: a Python Kafka producer generates synthetic credit card transaction events at 50,000 TPS with 2% injected fraud, publishes to Kafka topic `raw_transactions`. In Azure, this connects to Azure Event Hub which exposes a Kafka-compatible API — so the same Kafka consumer code works without changes. That's a key architectural decision: I kept it portable, not locked to Azure-specific Event Hub SDK.**

**The batch path uses Azure Data Factory with Copy Activities pulling from Salesforce REST API, Business Central ERP exports, and AWS S3 historical Parquet files. Both paths land in ADLS Gen2.**

**Now the Medallion Architecture on Databricks. Bronze is a Structured Streaming readStream consuming from Event Hub, writing in append mode to a Delta Lake table every 30 seconds. It's a raw archive — no schema enforcement, every byte preserved, partitioned by ingestion date. This is your audit trail and replay source.**

**Silver is where the transformation happens. I do a Delta MERGE — not overwrite — so it's idempotent and safe to replay. The logic is: parse timestamps, bounds-check amounts, deduplicate by taking the highest Kafka offset per transaction ID using a Window function — that converts Kafka's at-least-once delivery into exactly-once semantics without needing Kafka transactions. Then I flag data quality — DQ_PASSED Boolean — bad rows are flagged, never dropped. Finally feature engineering: hour, day of week, is-weekend, is-late-night, and velocity features that feed the ML model.**

**Gold runs Spark SQL aggregations into five tables — hourly summary, merchant daily, customer risk, geographic analysis, card performance. Each table uses MERGE on its natural key so they're fully idempotent.**

**The intelligence layer sits on top of Gold. The MLlib GBT model trains on 12 features with oversampling to handle the 2% fraud class imbalance, hits 0.97 AUC-ROC with 91% recall. MLflow tracks every run, manages the model registry, and the batch scoring job writes fraud scores back to Silver via MERGE.**

**The GenAI layer — Azure OpenAI GPT-4o reads Gold Delta tables, builds a structured JSON context with top fraud patterns and anomalies, and generates a 5-section executive briefing. I also built a Chroma vector database with 500K fraud alert embeddings using text-embedding-ada-002, so analysts can do natural language semantic search. A LangChain Agent with four custom tools monitors pipeline health, null rates, freshness, and explains DQ issues in plain English to the operations team.**

**Snowflake gets the Gold data via the Spark Connector. MERGE stored procedures keep it idempotent, Snowflake Tasks schedule hourly refreshes, and Snowpark Python computes RFM customer segments. Power BI hits it DirectQuery.**

**The whole thing deploys with Terraform in 12 minutes and GitHub Actions handles lint, test, validate, and deploy on every merge to main."**

---

## Technical Interview Q&A

### Streaming & Kafka

**Q: Why did you choose Kafka over Azure Event Streams or Kinesis?**

A: "Three reasons. Kafka is the industry standard for financial services — most banks and payment processors already produce to Kafka endpoints. Azure Event Hub exposes a Kafka-compatible API, so the producer code is portable — you can swap Azure Event Hub for Confluent Cloud or on-prem Kafka without touching application code. Second, Kafka's partition model maps directly to Spark's parallel consumption. 12 partitions = 12 parallel task streams in Databricks. Third, Kafka offsets give me exactly-once semantics in Silver — I store the max offset per transaction ID and deduplicate using a Window function. That's a production pattern, not a demo shortcut."

**Q: How do you handle Kafka at-least-once delivery to ensure no duplicate transactions in Silver?**

A: "Kafka guarantees at-least-once delivery — a consumer can receive the same message more than once after a rebalance or restart. In Silver, I deduplicate using a PySpark Window function: `PARTITION BY transaction_id ORDER BY kafka_offset DESC`, then `ROW_NUMBER() = 1`. This keeps the highest-offset record per transaction ID. Combined with Delta MERGE, which only updates when the source offset is higher than the stored offset, the pattern is idempotent — you can replay the entire Bronze table into Silver and get identical results."

---

### Delta Lake & Medallion Architecture

**Q: Why Delta Lake instead of Parquet directly?**

A: "Delta Lake gives you three things Parquet doesn't. ACID transactions — you can MERGE without corrupting partially-written files. Time-travel — every Bronze and Silver table has a full version history, so you can `SELECT * FROM bronze.transactions VERSION AS OF 5` to replay a historical state for debugging or regulatory audit. And schema enforcement/evolution — you can add columns to Gold without breaking downstream consumers. For financial data, the audit capability alone justifies it."

**Q: Why does Bronze never delete or update rows?**

A: "Bronze is your immutable source of truth — every byte you ever received, preserved exactly as received. If your Silver transformation logic has a bug and corrupts downstream tables, you can reprocess from Bronze and rebuild Silver and Gold from scratch. If a regulator asks 'what was the raw transaction data for customer X on date Y', Bronze has it. Deleting from Bronze would be like deleting your original paper receipts."

**Q: What is the data quality strategy in Silver?**

A: "I use a flag pattern rather than a drop pattern. Every row in Silver gets a `dq_passed` Boolean. Rows that fail — null transaction ID, null customer ID, negative amount, unparseable timestamp — are flagged False but kept in the table. This is deliberate: dropping bad rows silently means you don't know how many bad records you received or why. By keeping flagged rows, the DQ agent can analyze them, the operations team can investigate root causes, and the audit log shows exactly what happened. Gold aggregations simply filter `WHERE dq_passed = true`."

---

### Snowflake

**Q: Why Snowflake in addition to Delta Lake? Isn't that redundant?**

A: "Different tools for different consumers. Databricks Delta Lake is optimized for large-scale distributed processing — PySpark jobs, ML training, Structured Streaming. Snowflake is optimized for SQL analytics and BI tooling. Power BI DirectQuery to Snowflake on a Medium warehouse returns in under 2 seconds. DirectQuery to Databricks SQL has more latency and cost complexity. In real financial services architectures, you'd see exactly this pattern — Databricks or Spark for processing, Snowflake or Redshift as the serving layer for BI. The Spark Connector handles the sync."

**Q: What are Snowflake Tasks and why did you use them?**

A: "Snowflake Tasks are native scheduled SQL/Stored Procedure execution inside Snowflake — no external orchestrator needed. I use them to call the Gold aggregation stored procedures hourly, completely independent of Databricks. If the Databricks job runs late or fails, the Snowflake Task will still try to refresh Gold from whatever data is already in the staging table. It's a resilience layer — two independent paths to keep the mart fresh."

---

### ML & MLflow

**Q: Why GBT over XGBoost or deep learning for fraud detection?**

A: "Three practical reasons. PySpark MLlib's GBT is natively distributed — it trains in parallel across the Databricks cluster without data collection overhead. XGBoost requires `xgboost4j-spark` with version matching complexity. Deep learning would require TensorFlow or PyTorch on Databricks, adding infrastructure complexity and interpretability challenges — and financial services regulators often require model explainability, which tree-based models provide natively via feature importance. GBT at 0.97 AUC-ROC is also competitive with XGBoost for tabular fraud data in published benchmarks."

**Q: How did you handle the class imbalance (2% fraud rate)?**

A: "I used oversampling — duplicated fraud class rows 5x in the training set. The alternative is undersampling majority class or using class weights. I chose oversampling because it's simple to reason about and I had abundant data. The risk is overfitting to the oversampled patterns — I validated with a stratified 80/20 split to ensure the test set maintains the real 2% distribution and the AUC-ROC score reflects real-world performance."

---

### GenAI / LangChain

**Q: Why RAG instead of just prompting GPT-4o directly?**

A: "GPT-4o's context window is finite. I have 500K fraud alert records — I can't send them all in one prompt. RAG solves this: I embed every fraud alert as a vector using text-embedding-ada-002, store them in Chroma, and at query time retrieve the top-K most semantically similar records. Those K records become the context window. More importantly, RAG grounds the LLM in actual data — without it, GPT-4o might fabricate plausible-sounding fraud statistics. The executive briefing must be factually accurate, so grounding is non-negotiable."

**Q: What is the LangChain Agent doing vs. just calling GPT-4o?**

A: "A plain GPT-4o call is a one-shot prompt → response. A LangChain Agent can reason and take multiple actions. My DQ Agent has four tools it can call in any order: check pipeline run logs, check null rates, check freshness, explain a DQ issue. When you invoke it, it thinks: 'First I should check if pipelines ran successfully. If they did, I should check null rates. If null rates look abnormal, I should call explain_dq_issue to generate a human-readable explanation.' That multi-step reasoning and tool orchestration is what an Agent enables — you get an intelligent analyst, not just a text generator."

---

### Infrastructure & CI/CD

**Q: Walk me through the CI pipeline.**

A: "Four jobs. Lint Python — ruff checks for style, unused imports, undefined variables; black and isort are advisory (non-blocking) since reformatting is handled in pre-commit hooks. Unit Tests — pytest with a local PySpark session in `local[2]` mode; I deliberately excluded Delta Lake catalog extensions from the test SparkSession because Delta requires JARs that aren't on the CI Java classpath — the tests cover transformation logic which doesn't need actual Delta. Terraform Validate — `terraform init -backend=false` then `terraform validate` to catch HCL syntax and resource configuration errors without needing real Azure credentials. SQL Lint — sqlfluff against the Snowflake dialect. All four must pass before a PR can merge."

**Q: Why Terraform for infrastructure?**

A: "Three reasons that matter in production. Reproducibility — you can spin up a complete identical environment (dev, staging, prod) with a single `terraform apply`. Auditability — every infrastructure change is a PR through git, with a plan diff reviewed before apply. Disaster recovery — if the Azure subscription is lost or corrupted, you can provision everything from scratch in 12 minutes with `terraform apply`. Clicking through the Azure portal is not repeatable and not auditable."

---

### Behavioral / Design

**Q: What was the hardest technical problem you solved in this project?**

A: "The Kafka deduplication problem. Kafka guarantees at-least-once delivery, so under failure conditions you can get duplicate messages. If I just wrote every Kafka message to Silver, I'd have duplicate transactions inflating fraud counts and dollar volumes — catastrophic for a financial platform. The solution I chose — Window function deduplication on `kafka_offset DESC` combined with Delta MERGE — is the production pattern used in real financial streaming platforms. It took me time to understand why the offset-ordering matters: you want to keep the last-seen record, not the first, because a retry might have corrected fields that were corrupted in the first publish."

**Q: How would you scale this to 10x the current throughput?**

A: "Event Hub: increase throughput units (auto-inflate is already configured) and partition count to 120. Databricks: scale from the current auto-scaling 2–8 node cluster to a larger pool — Structured Streaming scales horizontally with Spark parallelism. Silver MERGE: partition Silver table by `transaction_date` so each MERGE batch only touches the current day's partition rather than the full table. Gold aggregations already run incrementally. Snowflake: scale up the warehouse during peak batch windows, scale down during off-hours using Resource Monitors. None of these changes require architecture changes — just configuration adjustments."

**Q: What would you add if you had another two weeks?**

A: "Two things with immediate business value. First, online ML scoring: deploy the GBT model to Databricks Model Serving as a REST endpoint, call it synchronously per transaction in the Kafka consumer, and write the fraud score to Bronze alongside the raw event. Right now scoring is batch — you'd catch fraud 15 minutes after the transaction. Synchronous scoring catches it at swipe time. Second, dbt Core for Gold transformations: replace the Spark SQL aggregation notebooks with dbt models. You get automatic data lineage documentation, column-level tests, and a version-controlled SQL transformation layer that SQL analysts can contribute to without knowing PySpark."

---

## Talking Points for Common Scenarios

### "You don't have a direct title match to Senior Data Engineer"
*"FinStream360 was built to demonstrate exactly what a Senior Data Engineer does day-to-day at a financial services company: streaming pipeline architecture, distributed processing at scale, ML integration, GenAI layer, cloud IaC, and CI/CD. The architecture follows the same patterns deployed at Tier-1 banks — medallion architecture on Delta Lake, Snowflake as the serving layer, Terraform for reproducibility. I can walk through any component in depth."*

### "We use [different cloud / Fabric / Redshift]"
*"The architectural patterns transfer directly. Medallion Architecture works identically on Microsoft Fabric with OneLake as the lakehouse — it's the next evolution of the Databricks + ADLS pattern. Snowflake's concepts — MERGE stored procedures, Tasks, Snowpark — have direct equivalents in Redshift (stored procedures, schedulers, Redshift ML). And Azure Data Factory, Databricks, and Kafka all have AWS equivalents I understand: MWAA, EMR/Glue, MSK."*

### "We're concerned about the depth of real production experience"
*"Every design decision in FinStream360 was made for production reasons, not simplicity. I chose at-least-once Kafka with Window-function deduplication instead of exactly-once Kafka transactions because the latter requires Kafka 2.5+ and transactional producers — most financial services teams avoid that complexity. I chose flag-and-keep for DQ over drop-and-log because regulators require audit trails of every received record. I chose Terraform with remote state backend instead of local state because production teams need state locking across team members. These are production-engineer decisions, not portfolio shortcuts."*

---

## Questions to Ask the Interviewer

1. "What does your current Bronze-to-Silver latency look like, and what's the team's SLA target for real-time data?"
2. "Are you running Databricks Workflows or Apache Airflow for orchestration — and is there a migration discussion happening?"
3. "How does the team handle schema evolution in streaming pipelines — auto-merge schemas or strict enforcement with dead-letter queues?"
4. "Is the team moving toward Microsoft Fabric / OneLake, and what's the migration timeline from ADLS + Databricks?"
5. "What does your MLflow or model registry setup look like — are models deployed for real-time scoring or batch?"
