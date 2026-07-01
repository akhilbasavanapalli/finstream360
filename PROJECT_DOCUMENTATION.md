# 📖 FinStream360 — Complete Project Documentation

> **Who is this document for?**
> This documentation is written for everyone — whether you are a data engineer, a hiring manager, a student learning data engineering, or someone with no technical background at all. Every section is written in plain English first, with technical details added after. You do not need to understand code to understand what this project does and why it matters.

---

## 📌 Table of Contents

1. [What Is FinStream360?](#1-what-is-finstream360)
2. [Why Was This Built?](#2-why-was-this-built)
3. [How the Pipeline Works — Plain English](#3-how-the-pipeline-works--plain-english)
4. [Project Folder Structure](#4-project-folder-structure)
5. [Root-Level Files](#5-root-level-files)
6. [.github/ — Automated Quality Checks](#6-github--automated-quality-checks)
7. [data_generation/ — Creating the Data](#7-data_generation--creating-the-data)
8. [docker/ — Local Development Environment](#8-docker--local-development-environment)
9. [docs/ — Documentation and Diagrams](#9-docs--documentation-and-diagrams)
10. [ingestion/ — Azure Data Factory Pipeline](#10-ingestion--azure-data-factory-pipeline)
11. [notebooks/ — The Core Data Pipeline + AI](#11-notebooks--the-core-data-pipeline--ai)
12. [snowflake/ — Snowflake Alternative](#12-snowflake--snowflake-alternative)
13. [terraform/ — Cloud Infrastructure as Code](#13-terraform--cloud-infrastructure-as-code)
14. [tests/ — Automated Testing](#14-tests--automated-testing)
15. [How Everything Connects](#15-how-everything-connects)
16. [Technology Stack — Explained Simply](#16-technology-stack--explained-simply)
17. [How to Run This Project](#17-how-to-run-this-project)

---

## 1. What Is FinStream360?

**In one sentence:** FinStream360 is a real-time data pipeline that processes credit card transactions, detects fraud, and delivers business intelligence dashboards — built using the same tools and patterns used by banks, fintech companies, and Fortune 500 enterprises.

**In plain English:** Imagine a bank processes millions of credit card swipes every day. Every swipe needs to be:
- Received and stored immediately (before the customer even leaves the store)
- Checked for quality (was the data complete? Was the amount valid?)
- Cleaned and organized (remove duplicates, fix formatting)
- Analyzed for fraud (is this transaction suspicious?)
- Summarized into reports (what is the fraud rate this hour? Which merchants are risky?)

FinStream360 does all of that — automatically, in real time, end to end.

---

## 2. Why Was This Built?

This project was built to demonstrate real-world, production-grade data engineering skills. It covers:

- **Streaming data** — handling data that arrives continuously, second by second
- **Cloud infrastructure** — deploying to Azure, the same cloud used by major banks
- **Data quality** — never losing data, always knowing which records are clean
- **Machine learning** — automatically scoring transactions for fraud risk
- **Generative AI** — using GPT-4 to write plain-English fraud reports
- **Infrastructure as Code** — building cloud environments with code, not mouse clicks
- **CI/CD automation** — automatically testing and deploying code every time changes are made

This is the kind of project that shows an employer: *"This person has built something real, not just followed a tutorial."*

---

## 3. How the Pipeline Works — Plain English

Think of it like a water purification system — water (data) enters dirty, moves through filters, and comes out clean and ready to drink (use).

```
Step 1: A fake card swipe happens
           ↓
Step 2: The transaction is sent to Apache Kafka
        (Kafka is like a post office — it holds messages until they're picked up)
           ↓
Step 3: Bronze Layer — Raw Storage
        (Every transaction saved exactly as received. Nothing changed. Nothing deleted.)
           ↓
Step 4: Silver Layer — Cleaning and Enriching
        (Fix the data: remove duplicates, flag bad records, add useful labels)
           ↓
Step 5: Gold Layer — Business Answers
        (Summarize into KPI tables that answer real business questions)
           ↓
Step 6: Machine Learning scores each transaction for fraud probability
           ↓
Step 7: AI (GPT-4) writes a plain-English fraud report automatically
           ↓
Step 8: Power BI reads the Gold tables and displays dashboards
```

This three-layer approach (Bronze → Silver → Gold) is called the **Medallion Architecture** and is the industry standard used by companies like Uber, Netflix, and major banks.

---

## 4. Project Folder Structure

Below is the complete map of every folder and file in this project.

```
finstream360/
│
├── 📄 README.md                          ← Project overview (GitHub homepage)
├── 📄 PROJECT_DOCUMENTATION.md           ← This file — complete documentation
├── 📄 requirements.txt                   ← Python libraries needed
├── 📄 pyproject.toml                     ← Python project configuration
├── 📄 .gitignore                         ← Files Git should never save
│
├── 📁 .github/workflows/                 ← Automated testing and deployment
│   ├── 📄 ci.yml                         ← Runs tests on every code push
│   └── 📄 cd.yml                         ← Deploys code when merged to main
│
├── 📁 data_generation/                   ← Creates fake transaction data
│   ├── 📄 transaction_producer.py        ← Streams transactions to Kafka (live)
│   ├── 📄 batch_csv_generator.py         ← Generates a CSV file of transactions
│   └── 📄 requirements.txt              ← Libraries for data generation only
│
├── 📁 docker/                            ← Local development environment
│   └── 📄 docker-compose.yml            ← Starts Kafka + MinIO on your laptop
│
├── 📁 docs/                              ← All documentation and diagrams
│   ├── 📄 ARCHITECTURE.md               ← Technical architecture with diagrams
│   ├── 📄 SKILLS_MATRIX.md              ← Skills demonstrated and where
│   └── 📁 dashboards/
│       └── 📄 powerbi_mockups.html      ← Visual dashboard mockups
│
├── 📁 ingestion/                         ← Azure Data Factory pipeline
│   └── 📁 adf_pipelines/
│       └── 📄 pl_ingest_transactions.json ← ADF pipeline exported as code
│
├── 📁 notebooks/                         ← Core PySpark pipeline code
│   ├── 📄 01_bronze_ingestion.py         ← Raw data ingestion layer
│   ├── 📄 02_silver_transformation.py    ← Data cleaning and enrichment
│   ├── 📄 03_gold_aggregations.py        ← KPI tables for business use
│   ├── 📄 04_ml_fraud_detection.py       ← Machine learning fraud model
│   ├── 📄 05_genai_fraud_insights.py     ← GPT-4 fraud narrative reports
│   └── 📄 06_ai_data_quality_assistant.py ← AI-powered data quality checks
│
├── 📁 snowflake/                         ← Same pipeline using Snowflake
│   ├── 📄 01_schemas_ddl.sql             ← Creates Snowflake tables
│   ├── 📄 02_stored_procedures.sql       ← Transformation logic in SQL
│   └── 📁 snowpark/
│       └── 📄 snowpark_etl.py            ← Python ETL using Snowpark API
│
├── 📁 terraform/                         ← Creates all Azure cloud resources
│   ├── 📄 main.tf                        ← Defines every Azure service to create
│   ├── 📄 variables.tf                   ← Configuration settings (names, region)
│   └── 📄 outputs.tf                     ← Prints URLs after resources are created
│
└── 📁 tests/                             ← Automated tests
    └── 📄 test_transformations.py        ← Tests that pipeline logic is correct
```

---

## 5. Root-Level Files

These files sit at the top level of the project and apply to everything.

---

### 📄 `README.md`

**What it is:** The front page of the GitHub repository.

**What it does:** When anyone visits the GitHub link for this project, the first thing they see is this file. It contains a summary of what the project does, a diagram of the architecture, a list of technologies used, instructions for running the project, and links to the dashboards.

**Non-technical analogy:** Think of it as the cover page and executive summary of a business report. It gives the reader everything they need to decide if they want to read further.

---

### 📄 `PROJECT_DOCUMENTATION.md`

**What it is:** This file — the complete reference guide.

**What it does:** Explains every single folder and file in plain English, suitable for both technical engineers and non-technical stakeholders. Goes into much more depth than the README.

---

### 📄 `requirements.txt`

**What it is:** A list of Python software libraries this project needs.

**What it does:** When someone clones this repository onto their computer, they run one command (`pip install -r requirements.txt`) and Python automatically installs every library the project needs — PySpark for data processing, kafka-python for Kafka connections, delta-spark for Delta Lake tables, faker for generating test data, and more.

**Non-technical analogy:** It's like a shopping list. Before you can cook the meal (run the project), you need to buy the ingredients (install the libraries). This file is the shopping list.

---

### 📄 `pyproject.toml`

**What it is:** A configuration file for Python development tools.

**What it does:** Configures `ruff` (a code style checker — it makes sure the code is clean and follows consistent formatting rules) and `pytest` (the test runner — it knows where to find the test files). This file tells those tools how to behave for this specific project.

**Non-technical analogy:** It's like the settings file for your car — it doesn't make the car go, but it controls how things like the dashboard, mirrors, and climate control behave.

---

### 📄 `.gitignore`

**What it is:** A list of files Git should never save or upload to GitHub.

**What it does:** Tells Git to ignore things like:
- `__pycache__/` — temporary Python files that get created automatically
- `.env` — files that contain passwords and secret keys
- `venv/` — your local Python environment (everyone creates their own)
- Large data files that don't belong in a code repository

**Non-technical analogy:** It's like telling a photocopier "don't copy the sticky notes and scrap paper, just the important pages."

**Why it matters:** Without this file, you could accidentally upload passwords to a public GitHub repo, which would be a serious security problem.

---

## 6. `.github/` — Automated Quality Checks

This folder contains instructions that GitHub follows automatically every time someone pushes code to the repository.

**Non-technical analogy:** Think of this as a quality inspector that sits at the door of your factory. Every time a worker tries to add something new to the production line, the inspector automatically checks it first. If the inspector finds a problem, nothing goes through.

---

### 📄 `.github/workflows/ci.yml` — Continuous Integration

**What it is:** The automated quality check that runs on every push to GitHub.

**What it does:** Every time a developer pushes code, GitHub automatically:
1. Sets up a clean Linux environment
2. Installs Python and all the project dependencies
3. Runs `ruff` to check the code style (catches typos, unused variables, bad formatting)
4. Runs `pytest` to execute all the unit tests
5. Reports success ✅ or failure ❌ directly on the GitHub commit

**Why it matters:** In a team environment, no one can accidentally break the main codebase without the CI check catching it first. This is standard practice at every serious technology company.

**Non-technical analogy:** Before a new employee's work gets included in the company's official process, their manager reviews it and checks it against quality standards. CI is the automated version of that review.

---

### 📄 `.github/workflows/cd.yml` — Continuous Deployment

**What it is:** The automated deployment process that runs when code is merged to the main branch.

**What it does:** After CI passes and code is approved, CD automatically deploys the updated notebooks and pipeline definitions to Azure. This means the latest version of the code is always running in the cloud without anyone having to manually upload files.

**Why it matters:** Manual deployments are error-prone and slow. CD ensures that the code in GitHub always matches what is running in production, and deployments are reproducible and consistent.

---

## 7. `data_generation/` — Creating the Data

This folder contains the scripts that generate fake (but realistic) credit card transaction data for testing and demonstrating the pipeline.

**Non-technical analogy:** Before testing a new car manufacturing line, engineers use test cars with simulated specifications. This folder creates the simulated data before a real bank's transaction feed is connected.

---

### 📄 `data_generation/transaction_producer.py`

**What it is:** A streaming data generator that sends live transactions to Apache Kafka.

**What it does:** When you run this script, it:
- Creates a pool of 200 realistic fake customers (each with a fixed customer ID, card type, and home state)
- Continuously generates one transaction every 0.2 seconds (5 per second)
- For each transaction, randomly decides if it is fraud (2% chance)
- Fraud transactions have telltale signs: large amounts ($500–$5,000), late-night hours (midnight to 3 AM), risky merchant categories (ATM withdrawals, online shopping, travel)
- Normal transactions have small amounts ($5–$300), daytime hours, everyday merchants (grocery, restaurant, pharmacy)
- Sends each transaction as a JSON message to the Kafka topic called `raw-transactions`
- Runs forever until you press Ctrl+C

**Key output:** A live stream of JSON messages flowing into Kafka, which the Bronze pipeline picks up every 10 seconds.

**Non-technical analogy:** This is like a cash register at a store that keeps ringing up purchases. Each beep is one transaction being sent to the system.

---

### 📄 `data_generation/batch_csv_generator.py`

**What it is:** A batch data generator that creates a CSV file of transactions.

**What it does:** Instead of streaming to Kafka, this script generates a large set of transactions (e.g., 50,000 rows) and saves them as a CSV file. This CSV file is then placed in the Azure Data Lake Storage landing zone, where the Azure Data Factory pipeline picks it up.

**When to use it:** When testing the ADF batch pipeline (data factory copying files) rather than the Kafka streaming pipeline.

**Non-technical analogy:** Instead of a live cash register stream, this is like exporting all of yesterday's transactions as a spreadsheet at the end of the day.

---

### 📄 `data_generation/requirements.txt`

**What it is:** A minimal list of libraries needed just for data generation.

**What it does:** Only two libraries are needed here — `kafka-python` (to send messages to Kafka) and `faker` (to generate realistic fake names, IDs, and amounts). This separate file means you can run data generation on a lightweight machine without installing the full PySpark stack.

---

## 8. `docker/` — Local Development Environment

This folder contains the configuration to run a complete cloud-like environment on your laptop using Docker.

**Non-technical analogy:** Docker is like a shipping container. Just as a shipping container can hold any goods and be moved anywhere in the world, a Docker container holds a software service and can run on any computer. This folder defines all the containers needed to simulate the cloud environment locally.

---

### 📄 `docker/docker-compose.yml`

**What it is:** A single configuration file that defines and starts 5 services at once.

**What it does:** Running `docker compose up -d` in this folder starts:

| Service | What It Is | What It Does |
|---|---|---|
| **Zookeeper** | Kafka's coordinator | Manages which Kafka broker is alive and keeps the cluster organized. Kafka cannot run without it. Runs on port 2181. |
| **Kafka** | The message broker | Receives transactions from the producer and holds them until the Bronze pipeline reads them. Accessible at localhost:9092 from your Mac and at kafka:29092 from inside Docker. |
| **Kafka UI** | A web dashboard | Open http://localhost:8080 in your browser to see all Kafka topics, browse messages, check how many messages are in the queue, and watch data arrive in real time. |
| **MinIO** | A local fake S3 / Azure Data Lake | Stores all the Bronze, Silver, and Gold Delta Lake files. PySpark treats it exactly like real cloud storage. API runs on port 9000; browser UI runs on port 9001. |
| **MinIO Setup** | One-time bucket creator | Runs once when you first start Docker, creates the four storage folders (bronze, silver, gold, checkpoints) inside MinIO, then exits. |

**Why this matters:** Without Docker, you would need an Azure or AWS account just to test your code. With Docker, the entire cloud environment runs on your laptop for free. You can develop, test, and break things without any cloud costs.

---

## 9. `docs/` — Documentation and Diagrams

This folder contains all human-readable documentation about the project's design, architecture, and skills.

---

### 📄 `docs/ARCHITECTURE.md`

**What it is:** A deep-dive technical architecture document.

**What it does:** Explains the technical decisions behind the project — why Kafka was chosen over other messaging systems, why Delta Lake was used instead of plain Parquet files, how the Medallion Architecture works, and what each component's role is. Contains Mermaid diagrams that render as visual flowcharts directly on GitHub.

**Non-technical analogy:** If the README is the brochure for a building, ARCHITECTURE.md is the architect's blueprint — explaining why each room is where it is and how all the pipes and wires connect.

---

### 📄 `docs/SKILLS_MATRIX.md`

**What it is:** A structured table mapping skills to the files that demonstrate them.

**What it does:** Lists every technology and skill used in this project (PySpark, Delta Lake, Kafka, Terraform, Machine Learning, GenAI, CI/CD, etc.) and points to the exact file in the repository that shows that skill in action.

**Why it matters:** When a recruiter or hiring manager reviews this project, they can quickly find evidence of a specific skill without reading every file. "Show me where you used Window functions in PySpark" → look up the skills matrix → go directly to `notebooks/02_silver_transformation.py`.

---

### 📄 `docs/dashboards/powerbi_mockups.html`

**What it is:** An HTML file showing what the Power BI dashboards look like.

**What it does:** Opens in any web browser to display five interactive dashboard mockups:
- **Fraud KPI Dashboard** — Real-time fraud rate, transaction volume, alert counts
- **Hourly Fraud Heatmap** — Which hours of the day have the highest fraud
- **Merchant Risk Scorecard** — Which merchant categories are riskiest
- **Customer 360 View** — Individual customer risk profile and history
- **Executive Summary** — High-level KPIs for leadership

**Why it matters:** Power BI dashboards cannot be embedded directly in GitHub. This HTML file makes the visual output of the entire pipeline visible to anyone who views the repository, without needing a Power BI license.

---

## 10. `ingestion/` — Azure Data Factory Pipeline

This folder contains the Azure Data Factory pipeline definition stored as code.

**Non-technical analogy:** Azure Data Factory is like a logistics coordinator — it doesn't handle the data itself, it schedules and manages who moves what, when, and where. This folder is the coordinator's written instructions.

---

### 📄 `ingestion/adf_pipelines/pl_ingest_transactions.json`

**What it is:** The full Azure Data Factory pipeline exported as a JSON file.

**What it does:** This file defines the entire ADF pipeline — `PL_FinStream360_ETL` — including:
- The **Copy Data activity** that moves raw data from the source system to the ADLS landing zone
- The **Databricks Notebook activities** for Bronze, Silver, and Gold (with all their parameters)
- The **If Condition activity** that checks the file exists before processing
- All **parameters** (load_date, source_system, environment)
- All **linked service references** (connections to storage and Databricks)
- **Retry settings** and **timeout configurations**

**Why it matters:** Storing the ADF pipeline as JSON in Git means the pipeline is version-controlled. If someone accidentally deletes or breaks the ADF pipeline in Azure, it can be restored from this file in seconds. It also means changes to the pipeline go through code review like any other code change.

---

## 11. `notebooks/` — The Core Data Pipeline + AI

This is the heart of the project. These six notebooks contain all the data transformation logic, machine learning, and AI capabilities.

**Why are they numbered?** The numbers define the execution order. `01` always runs before `02`, `02` before `03`, and so on. This makes the pipeline's sequence obvious at a glance.

---

### 📄 `notebooks/01_bronze_ingestion.py` — The Raw Storage Layer

**What it is:** The first transformation layer — raw data ingestion.

**What it does:**
1. Connects to the data source (Kafka stream or ADLS CSV file depending on the mode)
2. Reads incoming transactions exactly as they arrive — no changes to the data at all
3. Adds three audit columns to every row:
   - `ingestion_ts` — the exact timestamp when this row was processed
   - `ingestion_date` — today's date (used to partition data by day)
   - `source_system` — where the data came from (`kafka-local` or `azure_sql`)
4. Writes all rows to the Bronze Delta table (in append mode — nothing is ever deleted)
5. When running in streaming mode, processes a new batch every 10 seconds and saves a checkpoint so it can resume from where it left off if it crashes

**The Golden Rule of Bronze:** Never change, never delete, never filter. Every row that arrives goes in exactly as received. Bronze is the permanent audit trail.

**Non-technical analogy:** Think of Bronze like a CCTV recording. The camera records everything exactly as it happens, with a timestamp. You don't edit the footage. If something goes wrong later, you can always rewind and see exactly what happened.

**Key output:** A Delta Lake table called `bronze_transactions` containing every raw transaction ever received.

---

### 📄 `notebooks/02_silver_transformation.py` — The Cleaning Layer

**What it is:** The second transformation layer — data quality and enrichment.

**What it does:** Reads from Bronze and applies five transformations in sequence:

**Step 1 — Parse Types:** The timestamp column arrived as a plain text string like `"2026-06-30T02:15:30Z"`. Silver converts it to a proper timestamp data type so the system can do date arithmetic on it. Merchant names and card types are trimmed of extra spaces and converted to uppercase so `" visa "` and `"VISA"` are treated as the same thing.

**Step 2 — Data Quality (DQ) Flagging:** This is the most important rule in the entire pipeline: **never delete bad data, always flag it.** For every row, Silver checks: Is the transaction ID missing? Is the customer ID missing? Is the amount null, zero, or negative? Is the timestamp missing? If any check fails, the row gets `dq_passed = False`. If all checks pass, `dq_passed = True`. Every row stays in the table regardless — the Gold layer will simply only read the clean ones.

**Why never delete?** Because deleted data is gone forever. By flagging instead, operations teams can investigate why data failed, fix the source system, and reprocess. This is an audit requirement in regulated industries like banking and healthcare.

**Step 3 — Deduplication:** Kafka (the message system) can deliver the same message twice in certain failure scenarios. Each Kafka message has an offset number that keeps going up. If `transaction-ABC-123` arrives twice, the second delivery has a higher offset number. Using a Window function, Silver groups all rows by `transaction_id`, sorts them by offset (highest first), numbers them 1, 2, 3, and keeps only row number 1 — the latest delivery. Duplicates are removed safely without ever having to delete rows from Bronze.

**Step 4 — Feature Engineering:** Adds new columns that are derived from existing data and are needed for fraud detection:
- `txn_hour` — what hour was the transaction (0–23)
- `is_weekend` — was this a Saturday or Sunday
- `is_late_night` — was this between midnight and 5 AM (a strong fraud signal)
- `txn_day_of_week` — day number (1=Sunday, 7=Saturday)
- `txn_month` — the month number

**Step 5 — Delta MERGE (Upsert):** Instead of overwriting the Silver table each time, the notebook uses a MERGE operation — similar to SQL's UPSERT. For each incoming row: if a row with the same `transaction_id` already exists and the new version has a higher Kafka offset, update it. If the transaction is brand new, insert it. This makes running Silver multiple times safe — you always get the same result, no matter how many times you rerun.

**Key output:** A Delta Lake table called `silver_transactions` with clean, deduplicated, enriched data and a `dq_passed` boolean column on every row.

---

### 📄 `notebooks/03_gold_aggregations.py` — The Business Layer

**What it is:** The third transformation layer — business KPI tables.

**What it does:** Reads only the clean Silver rows (`dq_passed = True`) and runs three separate SQL aggregation queries to produce three Gold tables that directly answer business questions.

**Gold Table 1 — `gold_hourly_summary`**
Groups transactions by hour of day and card type. For each combination (e.g., "2 AM transactions on VISA cards"), it calculates: how many transactions, total dollar volume, how many were fraud, and the fraud rate as a percentage.

*Business question it answers:* "At what times of day is fraud highest, and which card types are most targeted?" → Risk teams use this to tighten authentication at high-risk hours.

**Gold Table 2 — `gold_merchant_performance`**
Groups transactions by merchant category and merchant state. Calculates transaction count, total revenue processed, fraud count, and fraud rate per merchant type per state.

*Business question it answers:* "Which types of merchants in which states have the highest fraud rates?" → Risk teams use this to flag or block high-risk merchant categories.

**Gold Table 3 — `gold_customer_risk`**
Groups transactions by customer ID — one row per customer. Shows their lifetime transaction count, total spending, how many times they've been flagged for fraud, and whether they've ever had a fraud incident.

*Business question it answers:* "How risky is each customer?" → This feeds the Customer 360 dashboard and powers real-time credit limit decisions.

**Key output:** Three Gold Delta tables that Power BI reads directly to display the fraud dashboards.

---

### 📄 `notebooks/04_ml_fraud_detection.py` — Machine Learning

**What it is:** A machine learning model that scores every transaction for fraud probability.

**What it does:**
1. Reads the Silver table (clean transactions with features already engineered)
2. Encodes categorical columns (card_type, merchant_category, merchant_state) into numbers that a machine learning model can understand
3. Assembles all features into a single feature vector
4. Trains a **Random Forest classifier** — a machine learning algorithm that builds many decision trees and takes a vote from all of them to classify each transaction
5. Each transaction gets a `fraud_probability` score between 0.0 and 1.0 — a score of 0.92 means the model is 92% confident this is a fraudulent transaction
6. Saves the scored transactions and the trained model for reuse

**What is a Random Forest?** Imagine you ask 100 experts to look at a transaction and each one votes "fraud" or "not fraud." The Random Forest takes the majority vote. Because each expert (tree) sees slightly different data, the combined result is much more reliable than any single expert alone.

**Non-technical analogy:** This is like a fraud analyst who has reviewed millions of past transactions and learned the patterns — "large amount, late night, ATM withdrawal, card not present = high risk." The model has learned those same patterns automatically from the data.

**Key output:** A column called `fraud_probability` added to each transaction, plus a saved model that can score new transactions in real time.

---

### 📄 `notebooks/05_genai_fraud_insights.py` — AI Fraud Reports

**What it is:** A notebook that uses Azure OpenAI (GPT-4) to write plain-English fraud analysis reports.

**What it does:**
1. Reads the Gold KPI tables to get the latest fraud statistics
2. Sends those statistics to GPT-4 with a prompt like: *"You are a senior fraud analyst. Here are this week's fraud statistics: [data]. Write a 3-paragraph executive summary explaining the key trends, what is driving them, and what actions the risk team should consider."*
3. GPT-4 returns a professionally written narrative report
4. The report is saved and can be emailed to stakeholders or displayed on a dashboard

**Example output (AI-generated):** *"This week, fraud activity concentrated heavily between midnight and 3 AM, with ATM_CASH transactions in Texas showing a 23% spike compared to last week. VISA cardholders were disproportionately targeted, accounting for 67% of fraud incidents despite representing only 40% of total transaction volume. The operations team should consider implementing step-up authentication for ATM transactions initiated after 11 PM and before 5 AM..."*

**Why this matters:** Data insights are only valuable if decision-makers can act on them. Most executives cannot read SQL query results or PySpark code. This notebook translates numbers into a story they can understand and act on — without a human analyst needing to write the report.

---

### 📄 `notebooks/06_ai_data_quality_assistant.py` — AI Data Quality

**What it is:** An AI-powered data quality auditor.

**What it does:**
1. Reads the Bronze table and runs data quality checks — null counts, value distributions, outlier detection, schema validation
2. Collects all DQ findings as statistics
3. Sends those findings to GPT-4 with a prompt like: *"You are a senior data engineer. Here are the data quality metrics for today's batch: [metrics]. Write a data quality report explaining what issues were found, how severe they are, and what the likely root cause is for each issue."*
4. GPT-4 returns a structured report with findings and recommendations
5. The report identifies things like: "12% of transaction_ts fields were null — this likely indicates the point-of-sale system in the 'TX' region had a clock synchronization issue between 2 AM and 4 AM."

**Why this matters:** Traditional data quality tools tell you *what* is wrong (e.g., "null_count = 47"). This AI assistant tells you *why* it might be wrong and *what to do about it* — dramatically reducing the time it takes to investigate and fix data issues.

---

## 12. `snowflake/` — Snowflake Alternative

This folder contains the same pipeline logic reimplemented for Snowflake — a different cloud data platform. Having both implementations demonstrates platform flexibility and depth of knowledge.

**Non-technical analogy:** It's like knowing how to cook the same recipe on both a gas stove and an electric stove. The dish is the same; the equipment is different.

---

### 📄 `snowflake/01_schemas_ddl.sql`

**What it is:** SQL commands that create the database structure inside Snowflake.

**What it does:** Creates:
- The Snowflake database (`FINSTREAM360_DB`)
- Three schemas (logical namespaces): `BRONZE`, `SILVER`, `GOLD`
- All tables within each schema with correct column types, nullability constraints, and clustering keys
- A `TRANSACTIONS_AUDIT` table for tracking pipeline runs

**Non-technical analogy:** This is like the blueprint for building the filing cabinet — defining every drawer, every folder, and every label before any paper goes in.

---

### 📄 `snowflake/02_stored_procedures.sql`

**What it is:** SQL procedures that run inside Snowflake to transform data.

**What it does:** Contains three stored procedures:
- `SP_BRONZE_TO_SILVER` — applies DQ flagging, type parsing, and deduplication logic in Snowflake SQL
- `SP_SILVER_TO_GOLD` — runs the three aggregations to produce the Gold KPI tables
- `SP_AUDIT_LOG` — records each pipeline run's status, row counts, and timestamps to the audit table

These are the same transformations as the PySpark notebooks — but written in Snowflake SQL and running inside Snowflake's compute engine instead of on a Spark cluster.

---

### 📄 `snowflake/snowpark/snowpark_etl.py`

**What it is:** A Python ETL script using the Snowpark library.

**What it does:** Snowpark is Snowflake's Python API — it lets you write Python code that runs inside Snowflake's engine (similar to how PySpark runs inside Spark). This script:
- Connects to Snowflake using the Snowpark session
- Reads the Bronze table as a Snowpark DataFrame
- Applies the same transformations as the PySpark Silver notebook (DQ, dedup, feature engineering)
- Writes results to Silver and Gold Snowflake tables

**Why both SQL and Snowpark?** SQL stored procedures are faster for simple aggregations. Snowpark Python is more flexible for complex logic. In real projects, you use both. This demonstrates knowing when to use each.

---

## 13. `terraform/` — Cloud Infrastructure as Code

This folder provisions every Azure cloud service needed for the pipeline — automatically, using code.

**Non-technical analogy:** Instead of logging into the Azure portal and clicking "Create Resource" dozens of times, Terraform lets you describe your infrastructure as code and then creates everything with one command. It's the difference between assembling furniture by hand versus having a machine that reads the blueprint and assembles it automatically.

---

### 📄 `terraform/variables.tf`

**What it is:** The configuration settings file for infrastructure.

**What it does:** Declares all the input variables used when creating cloud resources:
- `azure_region` — which Azure data center to use (e.g., `East US 2`)
- `resource_group_name` — the container that holds all Azure resources
- `storage_account_name` — the name for the ADLS Gen2 storage account
- `databricks_workspace_name` — name for the Databricks workspace
- `environment` — `dev`, `staging`, or `prod`
- `project_name` — used to prefix all resource names consistently

**Why this matters:** By using variables instead of hardcoded names, the same Terraform code can create a `dev` environment, a `staging` environment, and a `prod` environment — just by changing the variable values. This is called environment parity.

---

### 📄 `terraform/main.tf`

**What it is:** The main infrastructure definition — the most important Terraform file.

**What it does:** Defines every Azure resource to create:

| Resource | What It Is | What It Does |
|---|---|---|
| **Resource Group** | Azure container | Holds all other resources together |
| **ADLS Gen2 Storage** | Azure Data Lake | Stores all Bronze, Silver, Gold Delta files |
| **Storage Containers** | Folder structure | Creates the landing, bronze, silver, gold, checkpoints folders |
| **Azure Databricks** | Spark compute | The workspace where all notebooks run |
| **Azure Data Factory** | Orchestrator | Schedules and runs the pipeline |
| **Azure Key Vault** | Secrets manager | Stores passwords, tokens, connection strings securely |
| **Azure Event Hub** | Cloud Kafka | Managed Kafka service for production streaming |
| **Log Analytics** | Monitoring | Collects logs from all services for alerting and debugging |

Running `terraform apply` creates all of these in about 10 minutes. `terraform destroy` deletes them all cleanly.

---

### 📄 `terraform/outputs.tf`

**What it is:** Defines what Terraform prints after creating resources.

**What it does:** After `terraform apply` completes, this file tells Terraform to print useful values:
- The ADLS storage account URL (`abfss://finstream360@...`)
- The Databricks workspace URL (`https://adb-xxxx.azuredatabricks.net`)
- The Azure Data Factory instance name
- The Key Vault URL

These outputs are used by the CI/CD pipeline (in `.github/workflows/cd.yml`) to know where to deploy notebooks and pipeline definitions.

---

## 14. `tests/` — Automated Testing

This folder contains automated tests that verify the pipeline logic is correct. Tests run automatically in CI every time code is pushed to GitHub.

---

### 📄 `tests/test_transformations.py`

**What it is:** Unit tests for the core transformation logic.

**What it does:** Contains test functions that verify specific pieces of the pipeline work correctly:

- **`test_dq_flagging()`** — Creates a test DataFrame with some intentionally bad rows (null transaction_id, negative amount) and verifies that the DQ logic correctly sets `dq_passed = False` for those rows and `True` for clean rows.

- **`test_deduplication()`** — Creates a test DataFrame where the same `transaction_id` appears twice with different kafka offsets. Verifies that the dedup logic keeps only the row with the higher offset.

- **`test_feature_engineering()`** — Creates a transaction at 2 AM on a Sunday. Verifies that `is_late_night = True`, `is_weekend = True`, and `txn_hour = 2` are correctly calculated.

- **`test_gold_aggregation()`** — Runs the hourly aggregation on a small known dataset and verifies the output row count and fraud rate calculation are correct.

**Why automated tests matter:** Without tests, every code change carries risk — "I fixed the dedup logic but did I accidentally break the DQ flagging?" With tests, you run one command and immediately know if anything broke. In a team environment, no code gets merged without all tests passing.

**Non-technical analogy:** Tests are like quality checks on a production line. Before a car leaves the factory, it goes through automated checks — does the engine start? Do the brakes work? Do the lights come on? These tests answer the same question for the data pipeline: does each part work correctly?

---

## 15. How Everything Connects

Here is the complete picture of how all the files work together in a single pipeline run:

```
TRIGGER (daily schedule or file arrival)
    │
    ▼
ADF Pipeline [ingestion/adf_pipelines/pl_ingest_transactions.json]
    │
    ├── Step 1: Generate/receive data
    │   └── data_generation/transaction_producer.py  (streaming)
    │   OR data_generation/batch_csv_generator.py   (batch)
    │
    ├── Step 2: Raw storage
    │   └── notebooks/01_bronze_ingestion.py
    │       Reads from Kafka (running via docker/docker-compose.yml)
    │       Writes Delta to MinIO/ADLS
    │
    ├── Step 3: Clean and enrich
    │   └── notebooks/02_silver_transformation.py
    │       Reads Bronze → DQ Flag → Dedup → Features → Silver Delta
    │
    ├── Step 4: Aggregate KPIs
    │   └── notebooks/03_gold_aggregations.py
    │       Reads Silver → 3 SQL aggregations → 3 Gold Delta tables
    │
    ├── Step 5: Score for fraud
    │   └── notebooks/04_ml_fraud_detection.py
    │       Reads Silver → Random Forest model → fraud_probability scores
    │
    ├── Step 6: Generate AI report
    │   └── notebooks/05_genai_fraud_insights.py
    │       Reads Gold stats → GPT-4 → executive narrative report
    │
    └── Step 7: Audit data quality
        └── notebooks/06_ai_data_quality_assistant.py
            Reads Bronze → DQ metrics → GPT-4 → DQ report

MONITORING
    └── Azure Monitor + Log Analytics (provisioned by terraform/main.tf)
    └── CI/CD checks (.github/workflows/ci.yml + cd.yml)
    └── Automated tests (tests/test_transformations.py)
```

---

## 16. Technology Stack — Explained Simply

| Technology | What It Is | Why Used Here |
|---|---|---|
| **Apache Kafka** | A high-speed message delivery system — like a post office that never loses mail | Receives millions of card swipes per second and holds them until the pipeline is ready to process them |
| **Apache Spark / PySpark** | A distributed data processing engine — like a team of thousands of workers processing data in parallel | Processes huge volumes of transaction data fast; the industry standard for big data |
| **Delta Lake** | A storage format that adds database-like features (ACID transactions, rollback, time travel) on top of files | Ensures data is never corrupted, supports running the pipeline multiple times safely, enables auditing |
| **Azure Databricks** | A managed Spark platform on Azure | Runs PySpark notebooks without managing servers; used by banks and enterprises globally |
| **Azure Data Factory** | A cloud orchestration service | Schedules, triggers, and monitors every pipeline step; handles retries when something fails |
| **Azure Data Lake Storage Gen2** | Cloud file storage optimized for big data | Stores all Bronze, Silver, Gold Delta files in the cloud |
| **Azure Key Vault** | A secure secrets manager | Stores passwords and API keys so they never appear in code |
| **Docker + Docker Compose** | Containerization — runs software in isolated, portable environments | Lets the entire cloud stack run on a laptop for development and testing |
| **MinIO** | A local S3-compatible object store | Replaces cloud storage during local development; identical API to AWS S3 and Azure ADLS |
| **Snowflake** | A cloud data warehouse | Alternative platform; same pipeline demonstrated on a second system |
| **Terraform** | Infrastructure as Code tool | Creates all Azure resources automatically from code; eliminates manual portal work |
| **Azure OpenAI / GPT-4** | Large Language Model API | Generates human-readable fraud narratives and data quality reports |
| **Spark MLlib** | Machine learning library built into Spark | Trains and runs the fraud detection Random Forest model at scale |
| **GitHub Actions** | Automated CI/CD on GitHub | Runs tests and deploys code automatically on every push |
| **Python** | Programming language | Main language for all notebooks, scripts, and tests |

---

## 17. How to Run This Project

### Option A — Local Environment (Free, No Cloud Account Needed)

**Requirements:** Docker Desktop installed on your Mac/PC, Python 3.9+

```bash
# Step 1: Clone the repository
git clone https://github.com/YOUR_USERNAME/finstream360.git
cd finstream360

# Step 2: Install Python dependencies
pip install -r requirements.txt

# Step 3: Start the local environment (Kafka + MinIO)
cd docker
docker compose up -d

# Step 4: Verify everything is running
docker compose ps
# All 5 services should show "running"

# Step 5: Open Kafka UI in your browser
# http://localhost:8080

# Step 6: Open MinIO UI in your browser
# http://localhost:9001  (login: minioadmin / minioadmin123)

# Step 7: Start sending transactions to Kafka (in a new terminal)
cd data_generation
python transaction_producer.py

# Step 8: Start the Bronze streaming job (in another terminal)
cd notebooks
python 01_bronze_ingestion.py

# Step 9: After 30 seconds, run Silver transformation
python 02_silver_transformation.py

# Step 10: Run Gold aggregations
python 03_gold_aggregations.py
```

### Option B — Azure Databricks (Cloud)

1. Log in to your Azure Databricks workspace
2. Upload notebooks from the `notebooks/` folder to the Databricks workspace
3. Create a new cluster (or use Serverless)
4. Run the notebooks in order: 01 → 02 → 03 → 04 → 05 → 06

### Option C — Full Azure Infrastructure (Production)

```bash
# Requires Azure CLI and Terraform installed
cd terraform
terraform init
terraform plan
terraform apply   # Creates all Azure resources (takes ~10 minutes)

# Deploy notebooks via CI/CD
git push origin main   # GitHub Actions deploys automatically
```

---

## 📝 Summary

FinStream360 is a complete, production-grade data engineering portfolio project. Here is what makes it exceptional:

- **End-to-end:** From raw data generation all the way to AI-generated insights and Power BI dashboards
- **Two environments:** Runs locally with Docker AND in the cloud on Azure Databricks
- **Two platforms:** Implemented in both Databricks/PySpark AND Snowflake/Snowpark
- **Industry patterns:** Medallion Architecture, DQ flagging, idempotent MERGE, feature engineering — all patterns used in real enterprise data teams
- **AI-powered:** Machine learning fraud scoring and GPT-4 narrative reports built in
- **Production-ready:** CI/CD, automated tests, Terraform infrastructure, monitoring, alerting

---

*Documentation written by Akhil Reddy | June 2026 | FinStream360 v1.0*
