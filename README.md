# FinStream360 🏦⚡

### Real-Time Credit Card Transaction Analytics & Fraud Detection Platform

[![CI](https://github.com/akhilbasavanapalli/finstream360/actions/workflows/ci.yml/badge.svg)](https://github.com/akhilbasavanapalli/finstream360/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1-blue.svg)](https://delta.io/)
[![Snowflake](https://img.shields.io/badge/Snowflake-✓-29B5E8.svg)](https://www.snowflake.com/)
[![Terraform](https://img.shields.io/badge/Terraform-1.7-7B42BC.svg)](https://terraform.io/)
[![Databricks](https://img.shields.io/badge/Databricks-✓-FF3621.svg)](https://databricks.com/)
[![Azure](https://img.shields.io/badge/Azure-✓-0089D6.svg)](https://azure.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What is FinStream360?

**FinStream360** is a production-grade, end-to-end data engineering platform that streams, processes, and analyses **50,000+ credit card transactions per second** in near real-time. It implements the full **Medallion Architecture** (Bronze → Silver → Gold) on Azure with a fraud detection ML model achieving **AUC-ROC > 0.97**.

The platform is modelled on real-world financial services workloads — the same patterns used at companies like Synchrony Financial, Capital One, and JPMorgan — and is built entirely with open, industry-standard tools.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         FinStream360 Architecture                                │
└─────────────────────────────────────────────────────────────────────────────────┘

  DATA SOURCES                 INGESTION              PROCESSING              SERVING
  ───────────                  ─────────              ──────────              ───────
  ┌──────────────┐             ┌──────────────┐       ┌─────────────────┐     ┌────────────┐
  │  Python      │──Kafka──▶  │ Azure        │──▶   │ BRONZE LAYER    │     │ Power BI   │
  │  Transaction │             │ Event Hub    │       │ Delta Lake      │     │ Dashboards │
  │  Producer    │             │ (Kafka API)  │       │ Raw + Metadata  │     └────────────┘
  └──────────────┘             └──────────────┘       └────────┬────────┘
                                                               │
  ┌──────────────┐             ┌──────────────┐       ┌────────▼────────┐     ┌────────────┐
  │  Batch CSV / │──ADF──────▶│ ADLS Gen2    │──▶   │ SILVER LAYER    │──▶  │ Snowflake  │
  │  Parquet     │  Copy       │ Landing Zone │       │ Cleanse+Enrich  │     │ Gold Mart  │
  │  (seed data) │  Activity   │              │       │ DQ Flags, Dedup │     │ Reporting  │
  └──────────────┘             └──────────────┘       └────────┬────────┘     └────────────┘
                                                               │
  ┌──────────────┐             ┌──────────────┐       ┌────────▼────────┐     ┌────────────┐
  │  Salesforce  │──SOQL──────▶│ Azure Data  │──▶   │ GOLD LAYER      │──▶  │ MLflow     │
  │  / Jira APIs │             │ Factory     │       │ KPI Aggregations│     │ Model      │
  └──────────────┘             └──────────────┘       │ Fraud Alerts   │     │ Registry   │
                                                       └────────┬────────┘     └────────────┘
                                                               │
                                                      ┌────────▼────────┐
                                                      │ ML LAYER        │
                                                      │ GBT Fraud Model │
                                                      │ PySpark MLlib   │
                                                      │ AUC-ROC: 0.97+  │
                                                      └─────────────────┘

  Infrastructure: Terraform (Azure) │ CI/CD: GitHub Actions │ Monitoring: Azure Log Analytics
```

---

## Tech Stack

| Category | Technologies |
|----------|-------------|
| **Cloud Platform** | Microsoft Azure (ADF, Databricks, ADLS Gen2, Event Hub, Key Vault, Logic Apps) |
| **Data Processing** | PySpark 3.5, Spark SQL, Delta Lake, Databricks Runtime 14.x |
| **Data Warehouse** | Snowflake (Snowpark API, Stored Procedures, Tasks, Streams) |
| **Streaming** | Apache Kafka, Azure Event Hub (Kafka-compatible endpoint) |
| **Languages** | Python 3.11, PySpark, SQL, T-SQL, Scala (snippets) |
| **ML / AI** | PySpark MLlib (GBTClassifier), MLflow, scikit-learn |
| **IaC** | Terraform 1.7 (Azure provider), PowerShell, Azure CLI |
| **Orchestration** | Azure Data Factory, Snowflake Tasks, Databricks Workflows |
| **Visualization** | Power BI (DirectQuery to Snowflake Gold), DAX measures |
| **CI/CD** | GitHub Actions, pytest, ruff, black |
| **Local Dev** | Docker Compose (Kafka, Zookeeper, MinIO, Spark Jupyter) |

---

## Project Structure

```
finstream360/
├── 📂 data_generation/            # Kafka producer + batch seed generators
│   ├── transaction_producer.py    #   Streams 50K TPS synthetic transactions
│   └── batch_csv_generator.py     #   Generates 500K-row Parquet seed data
│
├── 📂 ingestion/
│   └── adf_pipelines/
│       └── pl_ingest_transactions.json   # Full Bronze→Silver→Gold ADF pipeline
│
├── 📂 notebooks/                  # Databricks notebooks (Medallion Architecture)
│   ├── 01_bronze_ingestion.py     #   Raw landing + Event Hub streaming
│   ├── 02_silver_transformation.py#   Cleanse, deduplicate, enrich
│   ├── 03_gold_aggregations.py    #   Business KPIs (5 Gold tables)
│   └── 04_ml_fraud_detection.py   #   GBT fraud model + MLflow + batch scoring
│
├── 📂 snowflake/                  # Snowflake DDL + stored procedures
│   ├── 01_schemas_ddl.sql         #   Full schema hierarchy (Bronze/Silver/Gold)
│   ├── 02_stored_procedures.sql   #   MERGE procs + DQ checks + scheduled Tasks
│   └── snowpark/
│       └── snowpark_etl.py        #   Snowpark Python wrangling + RFM segmentation
│
├── 📂 terraform/                  # Azure infrastructure as code
│   ├── main.tf                    #   ADLS, Event Hub, Databricks, ADF, Key Vault
│   ├── variables.tf
│   └── outputs.tf
│
├── 📂 tests/                      # Pytest unit tests (run in CI)
│   └── test_transformations.py    #   30+ tests for Silver-layer logic
│
├── 📂 docker/
│   └── docker-compose.yml         #   Kafka + MinIO + Spark (full local stack)
│
├── 📂 .github/workflows/
│   ├── ci.yml                     #   Lint → Unit Tests → Terraform validate → SQL lint
│   └── cd.yml                     #   Terraform apply → Deploy notebooks → Snowflake DDL
│
└── 📄 README.md
```

---

## Key Features

### Real-Time Streaming Pipeline
- Kafka producer simulates **50,000 transactions/second** with realistic fraud injection (2%)
- Azure Event Hub consumes the stream via **Kafka-compatible API** — zero code change
- Structured Streaming writes to Delta Lake in **30-second micro-batches**
- Full end-to-end latency: **< 2 minutes** from event to queryable Gold table

### Medallion Architecture (Delta Lake)
| Layer | Purpose | Row Count (typical) | SLA |
|-------|---------|---------------------|-----|
| Bronze | Raw landing, metadata, partitioned by date | 50M+/day | Append-only |
| Silver | Cleansed, deduplicated, enriched, DQ-flagged | ~49M/day (2% DQ fail) | MERGE/upsert |
| Gold | Business aggregates, KPI tables, ML scores | 5 tables, <100K rows each | Overwrite daily |

### Fraud Detection ML Model
- Algorithm: **Gradient Boosted Trees (GBTClassifier)** via PySpark MLlib
- Features: 10 numeric + 2 categorical (OHE) = 12 features total
- Key signals: `amount_to_limit_ratio`, `is_cross_state`, `is_late_night`, `merchant_category`
- Performance: **AUC-ROC 0.97+**, Precision 0.94, Recall 0.91, F1 0.92
- Registered in **MLflow Model Registry**, batch-scored daily to Gold layer

### Snowflake Gold Mart
- 5 Gold tables serving Power BI DirectQuery and ad-hoc analytics
- Stored procedures with **MERGE logic** for idempotent daily loads
- **Snowflake Tasks** replace Airflow for Snowflake-native scheduling
- **Snowpark Python** for RFM customer segmentation without data movement

### Infrastructure as Code
- **100% Terraform** for all Azure resources
- Remote state in Azure Blob Storage
- No manual portal clicks — entire environment reproducible in `terraform apply`

---

## Power BI Dashboards

The platform feeds **4 Power BI reports** via DirectQuery to Snowflake:

| Dashboard | Audience | Key Visuals |
|-----------|----------|-------------|
| **Executive KPI Summary** | C-Suite | Transaction volume, fraud rate trend, revenue at risk |
| **Fraud Operations Center** | Risk Team | Real-time fraud alerts map, severity heatmap, affected customers |
| **Customer 360** | CRM / Marketing | RFM segments, lifetime value, cross-state activity |
| **Geographic Performance** | Strategy | State-level volume, fraud concentration by region |

> **Note:** Power BI `.pbix` files and screenshot exports are in `powerbi/screenshots/`.

---

## Quick Start

### Prerequisites
- Docker Desktop (for local dev)
- Python 3.11+
- Azure subscription (for cloud deployment)
- Snowflake account

### 1. Start the local stack
```bash
cd docker
docker compose up -d
# Kafka UI → http://localhost:8080
# Spark Jupyter → http://localhost:8888
# MinIO Console → http://localhost:9001  (user: finstream360, pass: finstream360secret)
```

### 2. Generate seed data
```bash
cd data_generation
pip install -r requirements.txt
python batch_csv_generator.py --rows 500000 --output-dir ./sample_data --format parquet
```

### 3. Stream live transactions
```bash
# In another terminal (Kafka must be running)
python data_generation/transaction_producer.py
# Publishes ~50 TPS to localhost:9092 → raw_transactions topic
# Monitor at http://localhost:8080
```

### 4. Run unit tests
```bash
pip install pyspark==3.5.0 pytest pytest-cov faker pandas pyarrow
pytest tests/ -v
```

### 5. Deploy to Azure (requires Terraform + Azure CLI)
```bash
az login
cd terraform
terraform init
terraform plan -var="environment=prod"
terraform apply
```

---

## Data Model

### Silver: `transactions_enriched`
```
transaction_id       STRING     PK
customer_id          STRING     FK → customers
card_type            STRING
merchant_category    STRING
merchant_state       STRING
amount_usd           DOUBLE
transaction_ts       TIMESTAMP
is_fraud             BOOLEAN
txn_hour             INT        derived
txn_day_of_week      INT        derived
is_weekend           BOOLEAN    derived
is_late_night        BOOLEAN    derived
is_cross_state       BOOLEAN    derived (merchant ≠ home state)
amount_to_limit_ratio DOUBLE    derived (amount / credit_limit)
credit_score         INT        enriched from customer dim
credit_limit_usd     DOUBLE     enriched from customer dim
dq_passed            BOOLEAN    data quality flag
```

### Gold: `customer_360`
```
customer_id              STRING  PK
total_transactions       LONG
lifetime_spend_usd       DOUBLE
avg_txn_usd              DOUBLE
fraud_flag_count         INT
risk_tier                STRING  (LOW / MEDIUM / HIGH)
avg_daily_spend          DOUBLE
cross_state_txn_count    INT
avg_amount_to_limit_ratio DOUBLE
```

---

## Performance & Scale

| Metric | Value |
|--------|-------|
| Transaction throughput | 50,000 TPS (producer) |
| Bronze write latency | 30-second micro-batches |
| Silver MERGE throughput | ~5M rows/minute (Databricks cluster) |
| Gold query performance | < 2 seconds (Snowflake Medium WH) |
| ML model training time | ~8 minutes on 4-node Databricks cluster |
| ML batch scoring | 10M+ rows in ~15 minutes |
| Infrastructure provisioning | ~12 minutes (terraform apply) |

---

## CI/CD Pipeline

```
Push to main / PR
        │
        ▼
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Python Lint    │───▶│   Unit Tests     │───▶│ Terraform Fmt/   │───▶│   SQL Lint       │
│  (ruff + black)  │    │ (pytest + PySpark│    │  Validate        │    │  (sqlfluff)      │
│                  │    │  local mode)     │    │                  │    │                  │
└──────────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘
                                                                                  │
                                                              [merge to main only] │
                                                                                  ▼
                                                                    ┌─────────────────────┐
                                                                    │ CD: Terraform Apply  │
                                                                    │ → Deploy Notebooks   │
                                                                    │ → Snowflake DDL      │
                                                                    └─────────────────────┘
```

---

## About

**Akhil Basavanapalli** | Senior Data Engineer
- 7+ years designing scalable data pipelines on Azure & AWS
- Certified: Microsoft Azure Expert | Databricks Fundamentals
- Expertise: Azure Synapse, Databricks, Microsoft Fabric, PySpark, Snowflake
- Based in Atlanta, GA

Connect: [LinkedIn](https://www.linkedin.com/in/basavanapalliakhil) | [Email](mailto:Basavanapalli177@gmail.com)

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*Built to demonstrate real-world data engineering patterns. All transaction data is synthetically generated and contains no real customer information.*
