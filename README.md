# AtliQ Commerce — End-to-End Data Engineering Capstone

An end-to-end data engineering capstone project covering **Batch Data Engineering** and **Real-Time Data Engineering**.

The project demonstrates ingestion, medallion architecture, transformation, data quality, incremental processing, idempotency, orchestration, analytics, streaming, and CI/CD.

---

# 📌 Project Overview

This repository contains two complementary phases.

## Phase 1 — End-to-End Batch Data Engineering

```text
Azure SQL + CSV Business Data
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

Phase 1 implements the complete batch data engineering lifecycle, including source ingestion, Bronze/Silver processing, dbt Gold modeling, data quality, nightly orchestration, idempotency validation, Fabric analytics, and GitHub Actions CI/CD.

## Phase 2 — Real-Time Data Engineering

```text
Python Event Producer
        ↓
Confluent Kafka
        ↓
atliq.orders.events
        ↓
Databricks Structured Streaming
        ↓
Bronze → Silver → Gold
        ↓
5-Minute Revenue Windows

Airflow → Quality Checks / OPTIMIZE / Daily Summary
```

Phase 2 adds a real-time processing lane using Kafka, Databricks Structured Streaming, Delta/Unity Catalog, and Airflow.

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

## Technology Stack

- Azure SQL
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake
- dbt Core
- Microsoft Fabric
- Power BI
- GitHub Actions
- Python
- SQL / PySpark

---

# Phase 1 — Milestones

| Milestone | Work Completed | Status |
|---|---|---|
| M1 — OLTP | Azure SQL schema, ETL control/watermark table, transaction simulator | ✅ Done |
| M2 — Ingestion | Metadata-driven ADF SQL/CSV ingestion and orchestration | ✅ Done |
| M3 — Silver | Databricks Bronze/Silver processing and incremental facts | ✅ Done |
| M4 — Gold/dbt | dbt staging, intermediate and mart models + tests | ✅ Done |
| M5 — Nightly Sync | Master orchestration, 2 AM trigger, retry/idempotency design | ✅ Done |
| M6 — Fabric | Gold availability and executive dashboard | ✅ Done |
| M7 — CI/CD | GitHub, GitHub Actions, dbt CI, pipeline audit logging | ✅ Done |

---

# 📁 Phase 1 Evidence

All Phase 1 evidence is retained in the repository under:

```text
Phase 1 End-to-End Batch Data Engineering_CB/evidence/
```

The evidence is organized by milestone.

## M1 — OLTP Evidence

```text
evidence/M1_OLTP/
└── Screenshot 2026-09-05 010230.png
```

M1 covers:

- Azure SQL operational schema
- Customers
- Products
- Orders
- Order items
- Payments
- ETL control/watermark mechanism
- Daily transaction simulator

Supporting implementation files include:

```text
sql/01_schema_ddl.sql
sql/07_etl_control_table.sql
python/daily_order_simulator.py
```

---

## M2 — Azure Data Factory Evidence

```text
evidence/M2_ADF/
├── pl_master_batch.json
├── pl_master_batch.png
├── pl_sql_to_adls.json
├── pl_sql_to_adls.png
├── pl_sql_to_raw.json
└── pl_sql_to_raw.png
```

Evidence covers:

- `pl_sql_to_raw`
- `pl_sql_to_adls`
- `pl_master_batch`
- Metadata-driven ingestion
- SQL/CSV ingestion
- Pipeline orchestration
- Pipeline execution

The master pipeline also records execution status through the Azure SQL audit mechanism.

---

## M3 — Databricks Bronze / Silver Evidence

```text
evidence/M3_SILVER/
├── 01_Bronze_to_Silver.html
├── 01_Bronze_to_Silver.png
└── 01_Bronze_to_Silver_2.ipynb
```

M3 demonstrates:

- Bronze processing
- Silver transformation
- Incremental processing
- Business-key `MERGE`
- Data enrichment
- Product and supplier information
- `run_date` batch traceability

The `fact_sales` grain is one row per order item.

---

## M4 — dbt Gold Evidence

```text
evidence/M4_GOLD_DBT/
├── DAG_Lineage.png
├── Screenshot 2026-09-05 012544.png
├── Screenshot 2026-09-05 012733.png
├── Screenshot 2026-09-05 012747.png
├── Screenshot 2026-09-05 012927.png
├── dbt_Sources.png
└── dbt_test.png
```

The dbt project contains:

### Staging

```text
stg_customers
stg_marketing_spend
stg_order_items
stg_orders
stg_payments
stg_products
stg_supplier_price_list
```

### Intermediate

```text
int_sales_enriched
```

### Marts

```text
dim_customer
dim_date
dim_product
fact_sales
fact_marketing_spend
```

### Validation

```text
13/13 dbt models passed
18/18 dbt tests passed
0 warnings
0 errors
0 skips
```

Tests include:

- Not-null checks
- Uniqueness checks
- Relationship checks

---

## M5 — Nightly Automation & Idempotency Evidence

```text
evidence/M5_NIGHTLY_SYNC/
├── Nightly Run.png
├── idempotency_run.png
├── idempotency_run1.png
└── nightly_run.png
```

M5 validates:

- Master batch orchestration
- 2:00 AM trigger
- End-to-end execution
- Retry-safe processing
- Idempotency

The same source state was processed twice.

Verified results:

| Metric | Run 1 | Run 2 |
|---|---:|---:|
| `fact_sales` row count | 798 | 798 |
| Gross revenue | 2,126,260.00 | 2,126,260.00 |
| Supplier cost | 1,363,271.87 | 1,363,271.87 |

The matching results demonstrate that rerunning the same batch does not create duplicate sales records or change the financial totals.

---

## M6 — Microsoft Fabric Evidence

```text
evidence/M6_FABRIC/
├── Atliq daahboard.pbix
├── Atliq daahboard.pdf
├── Dashboard.png
└── Data Model.png
```

The executive dashboard includes:

- Revenue trend by month
- Top products by revenue
- Top cities by revenue
- Revenue by category

The curated Gold data is exposed to Microsoft Fabric for analytics.

---

## M7 — GitHub CI/CD Evidence

```text
evidence/M7_CICD/
├── Screenshot 2026-09-05 014222.png
├── Screenshot 2026-09-05 141426.png
├── Screenshot 2026-09-05 141511.png
└── Screenshot 2026-09-05 141517.png
```

M7 demonstrates:

- Git version control
- GitHub repository
- GitHub Actions
- dbt CI
- Pull-request validation
- Databricks authentication through GitHub Secrets
- Pipeline audit logging

The ADF master pipeline records execution metadata such as pipeline name, run ID, start/end timestamps, and status.

---

# 🔗 Phase 1 Supporting Documentation

Phase 1 also contains the detailed implementation report:

```text
Phase 1 End-to-End Batch Data Engineering_CB/
├── README.md
├── Cdebasics_AtliQ_End_to_End_Data_Engineering_Project_Report.docx
└── Cdebasics_AtliQ_End_to_End_Data_Engineering_Project_Report.pdf
```

The report documents the architecture, milestones, evidence, issues, resolutions, technical learnings, and interview/client explanation.

---

# 🟢 Phase 2 — Real-Time Data Engineering

Location:

```text
Phase 2 - Real Time/
```

## Technology Stack

- Python
- Confluent Kafka
- Databricks Structured Streaming
- Delta Lake
- Unity Catalog
- Databricks SQL
- Apache Airflow
- Docker

---

# Kafka

Topic:

```text
atliq.orders.events
```

Message structure:

```text
key   → order_id
value → JSON event
```

Using `order_id` as the Kafka key keeps events for the same order associated with the same partitioning key.

---

# Bronze Streaming

Target:

```text
atliq.streaming.bronze_order_events
```

Kafka metadata preserved includes:

- Key
- Value
- Topic
- Partition
- Offset
- Timestamp

---

# Silver Streaming

Target:

```text
atliq.streaming.silver_order_events
```

Transformations include:

- Explicit schema
- JSON parsing
- Data type conversion
- `event_ts` timestamp conversion
- 10-minute watermark
- Deduplication using `event_id`

---

# Gold Streaming

Target:

```text
atliq.streaming.gold_revenue_5min
```

Only:

```text
event_type = 'payment_received'
```

events are used for revenue aggregation.

A 5-minute tumbling window calculates:

- Orders paid
- Revenue

---

# Streaming Checkpoints

Separate checkpoints are used for each stream:

```text
/Volumes/atliq/streaming/checkpoints/bronze
/Volumes/atliq/streaming/checkpoints/silver
/Volumes/atliq/streaming/checkpoints/gold
```

This allows each streaming stage to maintain independent processing state and progress.

---

# Airflow Operations

DAG:

```text
atliq_streaming_ops
```

Schedule:

```text
Hourly
```

Task chain:

```text
check_fresh_events
        ↓
optimize_tables
        ↓
refresh_daily_summary
```

## Data Quality

The freshness check fails when no events are available in the previous two hours:

```text
No fresh events found in the last 2 hours
```

This failure scenario was intentionally tested.

## Maintenance

`OPTIMIZE` is executed on the Silver and Gold streaming tables.

## Daily Summary

Target:

```text
atliq.streaming.gold_daily_summary
```

Contains:

- Orders placed
- Orders paid
- Orders cancelled
- Revenue

---

# 📁 Phase 2 Evidence

```text
Phase 2 - Real Time/evidence/
├── dq_failure_freshness_check.png
├── dq_failure_log.txt
└── successful_airflow_dag_run.png
```

The evidence demonstrates:

- Successful Airflow execution
- Data-quality failure when the producer is stopped
- Freshness validation behavior

---

# 📂 Complete Repository Structure

```text
AtliQ-Commerce-End-to-End-Capstone/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── Phase 1 End-to-End Batch Data Engineering_CB/
│   │
│   ├── evidence/
│   │   ├── M1_OLTP/
│   │   ├── M2_ADF/
│   │   ├── M3_SILVER/
│   │   ├── M4_GOLD_DBT/
│   │   ├── M5_NIGHTLY_SYNC/
│   │   ├── M6_FABRIC/
│   │   └── M7_CICD/
│   │
│   ├── dbt_project/
│   ├── sql/
│   ├── python/
│   ├── M1_OLTP/
│   ├── M2_ADF/
│   ├── M3_SILVER/
│   ├── M4_GOLD_DBT/
│   ├── M5_NIGHTLY_SYNC/
│   ├── M6_FABRIC/
│   ├── M7_CICD/
│   ├── README.md
│   ├── Cdebasics_AtliQ_End_to_End_Data_Engineering_Project_Report.docx
│   └── Cdebasics_AtliQ_End_to_End_Data_Engineering_Project_Report.pdf
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

# 🔐 Security

Sensitive credentials are excluded from Git.

The repository ignores:

```text
.env
*.env
```

Real credentials must never be committed.

Examples such as:

```text
.env.example
```

contain placeholders only.

Databricks CI credentials are supplied through GitHub Secrets.

---

# 📊 Project Status

## Phase 1

| Component | Status |
|---|---|
| OLTP | ✅ Complete |
| ADF Ingestion | ✅ Complete |
| ADLS Gen2 | ✅ Complete |
| Databricks Bronze | ✅ Complete |
| Databricks Silver | ✅ Complete |
| dbt Gold | ✅ Complete |
| Data Quality | ✅ Complete |
| Incremental Processing | ✅ Complete |
| Idempotency | ✅ Verified |
| Nightly Orchestration | ✅ Complete |
| Microsoft Fabric | ✅ Complete |
| CI/CD | ✅ Complete |

## Phase 2

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
| DQ Failure Scenario | ✅ Tested |

---

# 🎯 Key Learning Outcomes

This project demonstrates how a modern data platform combines:

```text
Batch Processing
       +
Real-Time Processing
       +
Data Transformation
       +
Data Quality
       +
Incremental Processing
       +
Idempotency
       +
Orchestration
       +
Analytics
       +
CI/CD
```

The project also demonstrates why different technologies are used for different responsibilities:

- **ADF** → ingestion and orchestration
- **ADLS Gen2** → durable cloud storage
- **Databricks** → distributed transformation and streaming
- **dbt** → SQL transformation, testing and documentation
- **Kafka** → event streaming
- **Airflow** → scheduled operational workflows
- **Fabric / Power BI** → analytics and visualization
- **GitHub Actions** → CI/CD

---

# ⚠️ Known Scope Items

Two Phase 1 enhancements were intentionally left as future work rather than presented as completed:

1. **New vs. Returning Customers** dashboard view
2. **Automated failure alerting** for the nightly pipeline

These are documented as follow-up enhancements.

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
