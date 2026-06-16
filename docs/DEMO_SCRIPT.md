# FinStream360 — 3-Minute Demo Video Script

> Complete narration, screen directions, and storyboard for a professional LinkedIn/portfolio video walkthrough.

---

## Video Overview

| | Detail |
|-|--------|
| **Total duration** | 3 minutes (180 seconds) |
| **Audience** | Technical recruiters, hiring managers, senior data engineers |
| **Tone** | Professional, confident, fast-paced |
| **Tools needed** | Screen recording (QuickTime / OBS), GitHub open in browser |
| **Thumbnail** | Use `finstream360_thumbnail_v2.png` |

---

## Pre-Recording Setup Checklist

- [ ] GitHub repo open at `github.com/akhilbasavanapalli/finstream360`
- [ ] README visible, scrolled to top
- [ ] GitHub Actions tab open in second browser tab (show CI badges)
- [ ] Architecture diagram open in third tab (`docs/ARCHITECTURE.md`)
- [ ] Notebook files visible in `notebooks/` directory
- [ ] Terraform `main.tf` ready to show briefly
- [ ] Font size: 140% zoom for readability
- [ ] Microphone tested, background noise minimized

---

## Script with Screen Directions

---

### SEGMENT 1 — Hook & Business Context (0:00–0:30)

**[SCREEN: GitHub repo homepage — README hero section with badges]**

**NARRATION:**
> "This is FinStream360 — a production-grade financial fraud detection platform I built end-to-end on Microsoft Azure. It processes fifty thousand credit card transactions per second, detects fraud with 97% AUC-ROC accuracy, and generates AI-written executive reports automatically using Azure OpenAI GPT-4o.

> The business problem it solves is real: traditional fraud systems run batch jobs hours after transactions happen. By then, fraud has already occurred. FinStream360 gets raw card swipes into a queryable analytics layer in under twenty minutes — the same architecture pattern deployed at Tier-1 banks.

> Let me walk you through the full stack."

**[ACTION: Slowly scroll the README down past the badges — let viewer see the tech stack table]**

---

### SEGMENT 2 — Architecture Overview (0:30–1:00)

**[SCREEN: Switch to `docs/ARCHITECTURE.md` — Mermaid diagram visible]**

**NARRATION:**
> "The architecture has six layers.

> Transactions flow from a Kafka producer at fifty thousand per second into Azure Event Hub — which exposes a Kafka-compatible API — then into Databricks Structured Streaming.

> From there, the data moves through a Medallion Architecture: Bronze is the raw immutable archive in Delta Lake. Silver cleanses, deduplicates using Kafka offsets for exactly-once semantics, and flags data quality. Gold runs five Spark SQL aggregations — hourly summaries, merchant performance, customer risk, geographic analysis, and card performance.

> The Gold tables feed three paths: a PySpark MLlib GBT model for fraud scoring, Azure OpenAI for generating executive briefings, and Snowflake for Power BI dashboards."

**[ACTION: Point at different boxes in the architecture diagram as you mention each layer — trace the data flow left to right]**

---

### SEGMENT 3 — Code Walkthrough (1:00–1:50)

**[SCREEN: Navigate to `notebooks/` in GitHub]**

**NARRATION:**
> "Six production-ready Databricks notebooks. Let me highlight the key ones."

**[ACTION: Click on `02_silver_transformation.py`]**

> "Silver transformation — this is where the engineering happens. I deduplicate using a Window function partitioned by transaction ID, ordered by Kafka offset descending. Row number one keeps the latest-offset record — that converts Kafka's at-least-once delivery into exactly-once semantics without needing Kafka transactions.

> Every row gets a DQ-passed flag. Rows that fail quality checks are flagged, never dropped — so the audit team can investigate every bad record."

**[ACTION: Scroll briefly to show the dedup Window function code, then navigate to `05_genai_fraud_insights.py`]**

> "The GenAI layer. This notebook reads the Gold Delta tables, builds a structured JSON context from the top fraud anomalies, sends it to GPT-4o, and gets back a five-section executive fraud briefing — automatically. Zero analyst hours. I also built a Chroma vector database with five hundred thousand fraud alert embeddings so analysts can search in plain English."

**[ACTION: Scroll to show the RAG section briefly]**

---

### SEGMENT 4 — Infrastructure & CI/CD (1:50–2:30)

**[SCREEN: Navigate to `terraform/main.tf` in GitHub]**

**NARRATION:**
> "All Azure infrastructure is Terraform. Eight resources — ADLS Gen2, Event Hub, Databricks workspace, Azure Data Factory, Key Vault, Log Analytics, VNet — provisioned in twelve minutes from a single terraform apply. No portal clicking, fully reproducible."

**[ACTION: Briefly scroll to show the resource blocks, then switch to GitHub Actions tab]**

> "GitHub Actions CI/CD — four stages on every push. Python lint with ruff and black. PySpark unit tests — fifteen-plus test cases covering the Silver transformation logic. Terraform validate to catch HCL errors. SQL lint against the Snowflake dialect. All four must pass before code merges."

**[ACTION: Click into a recent CI run and show the green checkmarks on each step]**

---

### SEGMENT 5 — Business Impact & Close (2:30–3:00)

**[SCREEN: Back to README — scroll to the Key Achievements table]**

**NARRATION:**
> "The business impact: 50,000 transactions per second, sub-twenty-minute end-to-end latency, AUC-ROC 0.97 with 91% fraud recall. That's nine out of ten fraud events caught. Estimated 2.4 million dollars in annual fraud prevented on a mid-size card portfolio.

> The AI layer saves two hours per week per risk analyst on manual report writing. The Terraform IaC means any environment recreates in twelve minutes — critical for disaster recovery compliance.

> This is available on my GitHub — the link is in the description. I'd love to discuss how this architecture applies to your team's data platform. Let's connect."

**[ACTION: End on the README badges — full repo visible]**

---

## Storyboard Summary

| Time | Screen | Narration Focus |
|------|--------|----------------|
| 0:00–0:10 | README hero + badges | Hook: "50K TPS, AUC-ROC 0.97, GPT-4o" |
| 0:10–0:30 | README tech stack table | Business problem + solution overview |
| 0:30–1:00 | Architecture Mermaid diagram | 6-layer walkthrough, trace data flow |
| 1:00–1:30 | `02_silver_transformation.py` | Window dedup, DQ flag pattern |
| 1:30–1:50 | `05_genai_fraud_insights.py` | RAG, GPT-4o briefings, Chroma |
| 1:50–2:10 | `terraform/main.tf` | 8 Azure resources, IaC |
| 2:10–2:30 | GitHub Actions — CI run | 4-stage pipeline, green checks |
| 2:30–3:00 | README — Key Achievements | Business impact numbers + CTA |

---

## LinkedIn Post Copy (to accompany video)

**Title:** I built a real-time financial fraud detection platform — here's the full stack

**Body:**
```
🏦 FinStream360 — my senior data engineer portfolio project is live on GitHub.

What it does:
→ Processes 50,000 credit card transactions/second
→ Medallion Architecture on Azure Databricks + Delta Lake
→ PySpark MLlib fraud model: AUC-ROC 0.97+, 91% recall
→ Azure OpenAI GPT-4o generates executive fraud briefings automatically
→ Snowflake Gold Mart → Power BI DirectQuery dashboards
→ 100% Terraform IaC + GitHub Actions CI/CD

Tech stack:
Azure · Databricks · PySpark · Delta Lake · Kafka · Snowflake
Azure OpenAI · LangChain · MLflow · Terraform · GitHub Actions

This is the architecture pattern used at Tier-1 banks — built end-to-end by one engineer.

Full repo: github.com/akhilbasavanapalli/finstream360
3-minute walkthrough: [video link]

Open to Senior Data Engineer opportunities in Atlanta/remote.
Drop a comment if you want to discuss the architecture 👇

#DataEngineering #Azure #Databricks #Snowflake #Kafka #MLflow #GenAI #OpenToWork
```

---

## Recording Tips

**Voice pace:** Speak at 80% of your natural pace — demo narration always feels slower to you than to viewers.

**Mouse movement:** Move the mouse slowly and deliberately to code sections. Fast mouse movement is disorienting on video.

**Zoom in:** For code sections, zoom the browser to 150% so viewers can read variable names.

**Pauses:** Add 1-second pauses when switching screens — editors can cut silence but can't add context.

**Re-record threshold:** If you stumble twice in one segment, rerecord that segment only — don't restart the full video.

**Editing:** Even iMovie trim + title cards makes a significant professional difference. At minimum, add: title card at 0:00 with project name, lower-third with your name at 2:45.

**Music:** Optional low-volume background music (corporate/neutral) lifts perceived quality significantly. Use royalty-free tracks from YouTube Audio Library.
