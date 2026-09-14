# AtliQ Commerce — End-to-End Data Engineering Capstone

An end-to-end data engineering capstone project built as part of the Codebasics Data Engineering learning journey.

This project demonstrates both **Batch Data Engineering** and **Real-Time Data Engineering**, using modern cloud data engineering technologies including Azure, Databricks, dbt, Microsoft Fabric, Kafka, Delta Lake, Structured Streaming, and Airflow.

---

## 📌 Project Overview

The project is divided into two complementary phases:

### Phase 1 — End-to-End Batch Data Engineering

A complete batch data platform that processes business data from source systems through ingestion, transformation, modeling, data quality, orchestration, and analytics.

**Pipeline:**

```text
Azure SQL
    ↓
Azure Data Factory
    ↓
ADLS Gen2
    ↓
Azure Databricks
    ↓
Bronze → Silver
    ↓
dbt Gold
    ↓
Microsoft Fabric / Power BI
```

### Phase 2 — Real-Time Data Engineering

A real-time processing lane that produces order events and processes them through Kafka and Databricks Structured Streaming.

**Pipeline:**

```text
Python Event Producer
        ↓
Confluent Kafka
        ↓
Kafka Topic
        ↓
Databricks Structured Streaming
        ↓
Bronze
        ↓
Silver
        ↓
Gold
        ↓
5-Minute Revenue Windows
```

Airflow is used for scheduled operational checks and maintenance.

---

# 🏗️ Overall Architecture

```text
                         ATLIQ COMMERCE
                              │
                ┌─────────────┴─────────────┐
                │                           │
                ▼                           ▼
        PHASE 1 — BATCH              PHASE 2 — REAL TIME
                │                           │
            Azure SQL                  Event Producer
                │                           │
        Azure Data Factory            Confluent Kafka
                │                           │
             ADLS Gen2               Databricks Streaming
                │                           │
           Databricks              Bronze → Silver → Gold
          Bronze / Silver                    │
                │                      Revenue Windows
               dbt                           │
                │                         Airflow
             Gold
                │
       Microsoft Fabric
                │
            Power BI
```

---

# 🔵 Phase 1 — Batch Data Engineering

Location:

```text
Phase 1 End-to-End Batch Data Engineering_CB/
```

## Pipeline

```text
Azure SQL
    ↓
Azure Data Factory
    ↓
ADLS Gen2
    ↓
Databricks Bronze
    ↓
Databricks Silver
    ↓
dbt Gold
    ↓
Microsoft Fabric / Power BI
```

## Technologies Used

- Azure SQL
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake
- dbt
- Microsoft Fabric
- Power BI
- GitHub Actions

## Key Components

- Source / OLTP in Azure SQL
- Data ingestion using Azure Data Factory
- Bronze and Silver processing in Databricks
- Business-oriented Gold models using dbt
- Data quality tests and validation
- Incremental processing
- Idempotency
- Scheduled orchestration
- Microsoft Fabric integration
- GitHub Actions CI/CD

---

# 🟢 Phase 2 — Real-Time Data Engineering

Location:

```text
Phase 2 - Real Time/
```

## Pipeline

```text
Python Event Producer
        ↓
Confluent Kafka
        ↓
atliq.orders.events
        ↓
Databricks Structured Streaming
        ↓
Bronze
        ↓
Silver
        ↓
Gold
```

## Kafka

Kafka topic:

```text
atliq.orders.events
```

Kafka messages use:

```text
key   → order_id
value → JSON event
```

Using `order_id` as the message key keeps events for the same order associated with the same Kafka partitioning key.

---

# 🥉 Bronze Streaming Layer

Kafka events are ingested into the Bronze layer while preserving raw event information and Kafka metadata.

The Bronze stream captures:

- Kafka key
- Kafka value
- Topic
- Partition
- Offset
- Timestamp

Target table:

```text
atliq.streaming.bronze_order_events
```

---

# 🥈 Silver Streaming Layer

The Bronze events are parsed and transformed into structured records.

The Silver layer includes:

- Explicit schema
- JSON parsing
- Data type conversion
- `event_ts` timestamp conversion
- 10-minute watermark
- Deduplication using `event_id`

Target table:

```text
atliq.streaming.silver_order_events
```

### Deduplication

Duplicate events are removed using:

```text
event_id
```

### Watermark

A 10-minute watermark is used to manage late-arriving events while allowing the streaming query to maintain bounded state.

---

# 🥇 Gold Streaming Layer

The Gold stream focuses on successful payments.

Only:

```text
event_type = 'payment_received'
```

events are used for revenue aggregation.

A **5-minute tumbling window** is applied.

The Gold layer calculates:

- Orders paid
- Revenue

Target table:

```text
atliq.streaming.gold_revenue_5min
```

Because streaming uses event-time processing and watermarks, Gold windows can appear after the corresponding events arrive and the watermark advances sufficiently.

---

# 📍 Streaming Checkpoints

Each streaming query has its own checkpoint location.

```text
/Volumes/atliq/streaming/checkpoints/bronze
/Volumes/atliq/streaming/checkpoints/silver
/Volumes/atliq/streaming/checkpoints/gold
```

Separate checkpoints allow each streaming stage to maintain its own processing state and progress.

---

# ⚙️ Airflow Operations

Phase 2 includes an Apache Airflow DAG:

```text
atliq_streaming_ops
```

The DAG runs hourly.

## DAG Structure

```text
check_fresh_events
        ↓
optimize_tables
        ↓
refresh_daily_summary
```

### Data Quality

The freshness check verifies that events have arrived in the Silver table within the previous two hours.

If no fresh events are found, the task fails with:

```text
No fresh events found in the last 2 hours
```

This failure scenario was intentionally tested.

### Table Maintenance

The `optimize_tables` task runs `OPTIMIZE` on the Silver and Gold streaming tables.

### Daily Summary

The final task rebuilds a daily summary containing:

- Orders placed
- Orders paid
- Orders cancelled
- Revenue

Target:

```text
atliq.streaming.gold_daily_summary
```

---

# 🔄 Databricks Streaming Environment

Phase 2 uses Databricks Free Edition with Unity Catalog.

Catalog:

```text
atliq
```

Schema:

```text
streaming
```

Streaming tables:

```text
atliq.streaming.bronze_order_events
atliq.streaming.silver_order_events
atliq.streaming.gold_revenue_5min
atliq.streaming.gold_daily_summary
```

Checkpoints are stored in a Unity Catalog Volume:

```text
atliq.streaming.checkpoints
```

---

# 🚀 CI/CD

GitHub Actions is used to validate the dbt pipeline.

Workflow:

```text
.github/workflows/ci.yml
```

The CI workflow:

1. Checks out the repository
2. Sets up Python
3. Installs dbt
4. Installs dbt dependencies
5. Validates Databricks connection configuration
6. Runs `dbt build`

Databricks credentials are provided through **GitHub Secrets** rather than being stored in the repository.

---

# 🔐 Security

Sensitive credentials are intentionally excluded from Git.

The root `.gitignore` excludes:

```text
.env
*.env
```

Real API keys, passwords, and Databricks tokens must never be committed.

Example configuration files such as:

```text
.env.example
```

contain placeholders rather than real credentials.

### Security Principles

- Never commit API keys
- Never commit passwords
- Never commit Databricks tokens
- Use environment variables for local secrets
- Use GitHub Secrets for CI/CD credentials
- Rotate credentials if they are accidentally exposed

---

# 📂 Repository Structure

```text
AtliQ-Commerce-End-to-End-Capstone/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── Phase 1 End-to-End Batch Data Engineering_CB/
│   ├── dbt_project/
│   ├── sql/
│   ├── evidence/
│   ├── M1_OLTP/
│   ├── M2_ADF/
│   ├── M3_SILVER/
│   ├── M4_GOLD_DBT/
│   ├── M5_NIGHTLY_SYNC/
│   ├── M6_FABRIC/
│   ├── M7_CICD/
│   ├── README.md
│   └── ...
│
├── Phase 2 - Real Time/
│   ├── airflow/
│   ├── databricks/
│   ├── evidence/
│   ├── producer/
│   └── Phase2_Writeup.md
│
├── .gitignore
└── README.md
```

---

# 📊 Phase 1 — Project Status

| Component | Status |
|---|---|
| Source / OLTP | ✅ Complete |
| Azure Data Factory | ✅ Complete |
| ADLS Gen2 | ✅ Complete |
| Databricks Bronze | ✅ Complete |
| Databricks Silver | ✅ Complete |
| dbt Gold | ✅ Complete |
| Data Quality | ✅ Complete |
| Incremental Processing | ✅ Complete |
| Idempotency | ✅ Complete |
| Nightly Orchestration | ✅ Complete |
| Microsoft Fabric | ✅ Complete |
| CI/CD | ✅ Complete |

---

# 📡 Phase 2 — Project Status

| Component | Status |
|---|---|
| Kafka Producer | ✅ Complete |
| Kafka Topic | ✅ Complete |
| Bronze Streaming | ✅ Complete |
| Silver Streaming | ✅ Complete |
| Gold Streaming | ✅ Complete |
| Watermarking | ✅ Complete |
| Deduplication | ✅ Complete |
| Separate Checkpoints | ✅ Complete |
| Airflow DAG | ✅ Complete |
| Successful DAG Run | ✅ Tested |
| Data Quality Failure Scenario | ✅ Tested |

---

# 🎯 Learning Objectives

This project demonstrates how a modern data platform can combine:

```text
Batch Data Engineering
        +
Real-Time Data Engineering
        +
Data Transformation
        +
Data Quality
        +
Orchestration
        +
CI/CD
```

The objective is not only to build individual pipelines, but to understand how different data engineering components work together in an end-to-end architecture.

---

# 📚 Documentation

Detailed documentation for each phase is available inside the respective project folders.

### Phase 1

```text
Phase 1 End-to-End Batch Data Engineering_CB/README.md
```

### Phase 2

```text
Phase 2 - Real Time/Phase2_Writeup.md
```

Additional implementation evidence and screenshots are available inside the `evidence` folders.

---

# 👨‍💻 Author

**Jaideep Gupta**

Tableau Developer | Data & Analytics Engineering Learner

---

## ⭐ Technologies

```text
Azure SQL
Azure Data Factory
ADLS Gen2
Azure Databricks
Delta Lake
dbt
Microsoft Fabric
Power BI
Confluent Kafka
Apache Airflow
Python
SQL
GitHub Actions
```
