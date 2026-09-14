<div align="center">

# 🛒 AtliQ Commerce

### End-to-End Data Engineering Platform — Batch + Real-Time

**Azure SQL · Azure Data Factory · ADLS Gen2 · Azure Databricks · dbt Core · Microsoft Fabric · Kafka · Airflow · GitHub Actions**

![CI](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)
![dbt models](https://img.shields.io/badge/dbt%20models-13%2F13%20passing-brightgreen?logo=dbt&logoColor=white)
![dbt tests](https://img.shields.io/badge/dbt%20tests-18%2F18%20passing-brightgreen?logo=dbt&logoColor=white)
![Idempotency](https://img.shields.io/badge/idempotency-verified-success)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-atliq-FF3621?logo=databricks&logoColor=white)
![Streaming](https://img.shields.io/badge/streaming-Kafka%20%2B%20Structured%20Streaming-231F20?logo=apachekafka&logoColor=white)

</div>

---

## 🗂️ Table of Contents

- [Project Overview](#-project-overview)
- [What This Project Demonstrates](#-what-this-project-demonstrates)
- [Business Problem](#-business-problem)
- [🔵 Phase 1 — Batch Data Engineering](#-phase-1--batch-data-engineering)
  - [Phase 1 Architecture](#phase-1-architecture)
  - [Source Data](#source-data)
  - [0. Project Foundation & OLTP Setup](#0-project-foundation--oltp-setup)
  - [1. OLTP – Azure SQL](#1-oltp--azure-sql)
  - [2. Ingestion – Azure Data Factory](#2-ingestion--azure-data-factory)
  - [3. Bronze – ADLS Gen2 / Databricks](#3-bronze--adls-gen2--databricks)
  - [4. Silver – Azure Databricks](#4-silver--azure-databricks)
  - [5. Gold Layer – dbt](#5-gold-layer--dbt)
  - [dbt Validation](#-dbt-validation)
  - [Data Quality & Reconciliation](#-data-quality--reconciliation)
  - [Nightly Batch & Reliability](#-nightly-batch--reliability)
  - [Microsoft Fabric / Power BI](#-microsoft-fabric--power-bi)
  - [CI/CD – GitHub Actions](#-cicd--github-actions)
  - [Phase 1 Evidence Gallery](#-phase-1-evidence-gallery)
- [🟢 Phase 2 — Real-Time Data Engineering](#-phase-2--real-time-data-engineering)
  - [Phase 2 Architecture](#phase-2-architecture)
  - [Kafka](#kafka)
  - [Bronze Streaming](#bronze-streaming)
  - [Silver Streaming](#silver-streaming)
  - [Gold Streaming](#gold-streaming)
  - [Streaming Checkpoints](#streaming-checkpoints)
  - [Airflow Operations](#airflow-operations)
  - [Phase 2 Evidence](#phase-2-evidence)
  - [Phase 2 Status](#-phase-2--real-time-project-status)
- [Phase 1 vs. Phase 2 — Two Independent Lanes](#-phase-1-vs-phase-2--two-independent-lanes)
- [Repository Structure](#-repository-structure)
- [Key Engineering Decisions](#-key-engineering-decisions)
- [Issues Faced & Resolutions](#-issues-faced--resolutions)
- [Security](#-security)
- [Local dbt Commands](#-local-dbt-commands)
- [Project Documentation](#-project-documentation)
- [Project Outcome](#-project-outcome)
- [Author](#-author)

---

## 🎯 Project Overview

AtliQ Commerce is an end-to-end analytical data platform built for an e-commerce business. The repository is organized into **two separate, independently deployable phases**:

- **Phase 1** moves data through a governed medallion architecture on a **nightly batch cadence** — Azure SQL/CSV → Azure Data Factory → ADLS Gen2 → Databricks → dbt → Microsoft Fabric/Power BI.
- **Phase 2** adds a **real-time lane** — Kafka → Databricks Structured Streaming → Bronze/Silver/Gold Delta tables → Airflow — that runs alongside Phase 1 without replacing or modifying it.

The repository includes implementation code, pipeline exports, dashboard artifacts, CI/CD configuration, data-quality evidence, and a full technical write-up for both phases.

---

## 🧭 What This Project Demonstrates

| Area | Demonstrated by |
| --- | --- |
| **Data modeling** | Normalized OLTP schema in Azure SQL → dimensional star schema in Gold |
| **Orchestration** | Metadata/control-table-driven ADF pipelines instead of one pipeline per table |
| **Distributed processing** | Databricks Bronze → Silver transformation, enrichment, incremental MERGE |
| **Analytics engineering** | dbt Core models, sources, tests, and documentation on Unity Catalog |
| **Data quality** | Automated dbt tests + manual reconciliation of revenue, cost and profit |
| **Reliability engineering** | Idempotency proven by re-running the full pipeline and diffing results |
| **BI delivery** | Microsoft Fabric / Power BI executive dashboard over curated Gold data |
| **DevOps** | GitHub Actions CI gated on dbt tests, secrets management, audit logging |
| **Streaming** | Kafka → Structured Streaming Bronze/Silver/Gold with watermarking & dedup |
| **Ops monitoring** | Airflow DAG for freshness checks, table maintenance and daily rollups |

---

## 💼 Business Problem

The operational database supports the e-commerce application, but leadership needs analytical answers such as:

- 📈 How is revenue changing over time?
- 🏆 Which products generate the most revenue?
- 🏙️ Which cities contribute the most revenue?
- 👥 How can customer and sales performance be analyzed?
- 💰 How can supplier cost be used to understand profitability?

Running heavy analytical queries directly against the live OLTP database risks degrading application performance. Phase 1 builds a separate analytical processing path and synchronizes data through a nightly batch, isolating reporting workloads from the transactional system. Phase 2 extends this with a near-real-time view of order and revenue activity, for questions that can't wait for the next nightly run.

---

# 🔵 Phase 1 — Batch Data Engineering

Phase 1 is the core analytical platform: a nightly batch pipeline that takes operational data from Azure SQL and CSV sources, moves it through a medallion architecture, and exposes a governed, tested Gold layer to Power BI / Microsoft Fabric.

📎 [**View Phase 1 in detail →**](https://github.com/Jaideepgupta/atliq-capstone-data-engineering)

## Phase 1 Architecture

![AtliQ Commerce Phase 1 Architecture](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/atliq_commerce_architecture.svg)

```text
Azure SQL / CSV
      ↓
Azure Data Factory      (metadata-driven ingestion & orchestration)
      ↓
ADLS Gen2 – Bronze      (durable raw landing)
      ↓
Azure Databricks – Silver   (cleansing, typing, enrichment, MERGE)
      ↓
dbt Core – Gold         (dimensional modeling + automated tests)
      ↓
Microsoft Fabric / Power BI  (executive reporting)
```

## Source Data

### Azure SQL – OLTP

The operational database contains:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`

### Marketing Spend (`marketing_spend.csv`)

```text
spend_date
channel
campaign
spend_amount
clicks
```

### Supplier Price List (`supplier_price_list.csv`)

```text
product_id
product_name
supplier_name
supplier_cost
effective_date
```

The supplier price list enriches product/sales data for cost and profitability analysis.

> **Design note:** Marketing spend is modeled separately from sales because the source data has no reliable sale-to-campaign attribution key. A date-only join could multiply sales when several campaigns or channels run on the same date.

## 0. Project Foundation & OLTP Setup

The project starts with a normalized Azure SQL OLTP schema for the e-commerce application. The repository includes the DDL used to create the operational tables and the ETL metadata/audit objects used by the ingestion framework. Seed/source data was loaded and validated before building the downstream analytical pipeline.

## 1. OLTP – Azure SQL

Azure SQL acts as the operational source system. The normalized OLTP model is designed for transactional workloads, with separate tables for customers, products, orders, order items and payments. The project also uses an ETL control table and watermark concepts to support incremental ingestion.

## 2. Ingestion – Azure Data Factory

Azure Data Factory handles source extraction and orchestration.

**Main pipelines:**

| Pipeline | Role |
| --- | --- |
| `pl_sql_to_raw` | SQL extraction and raw ingestion flow |
| `pl_sql_to_adls` | Landing/orchestration flow for ADLS-oriented Bronze storage |
| `pl_master_batch` | Master pipeline coordinating the end-to-end batch process; also records pipeline execution status through an Azure SQL audit procedure |

> ✅ Ingestion is **metadata-driven rather than hard-coded as one pipeline per source table**. An ETL control table defines the source table, load type and watermark information; the master flow reads that metadata to process the configured tables. Incremental SQL loads use the stored watermark and advance it only after a successful load. CSV sources are handled as full-refresh/overwrite inputs.

**High-level orchestration:**

```text
pl_sql_to_raw
      ↓
pl_sql_to_adls
      ↓
Databricks_Bronze_to_Gold
```

Pipeline JSON exports and implementation screenshots:
`Phase 1 End-to-End Batch Data Engineering_CB/evidence/M2_ADF/`

Supporting Azure SQL scripts:

- `sql/07_etl_control_table.sql`
- `sql/08_pipeline_audit.sql`

## 3. Bronze – ADLS Gen2 / Databricks

Bronze is the source-oriented landing layer. Its purpose is to:

- ✅ Preserve ingested source data as-is
- ✅ Separate ingestion from transformation
- ✅ Provide durable cloud storage independent of compute
- ✅ Provide a repeatable input for downstream processing

SQL-derived Bronze data is stored as Parquet. Marketing spend and supplier pricing CSVs are also ingested into Bronze as full-refresh inputs.

## 4. Silver – Azure Databricks

Azure Databricks performs the main distributed transformation and enrichment work. The Silver layer is responsible for:

- 🧹 Cleaning and standardizing source data
- 🔢 Applying appropriate data types
- 🔄 Transforming source structures
- 🔗 Enriching sales with product information
- 💵 Enriching sales with supplier cost
- ♻️ Applying incremental MERGE logic:
  - `orders` — latest record selected using `updated_at DESC`
  - `payments` — latest record selected using `updated_at DESC`
  - `order_items` — insert-only using `created_at`, since line items don't change after creation

Databricks implementation evidence:
`Phase 1 End-to-End Batch Data Engineering_CB/evidence/M3_SILVER/`

## 5. Gold Layer – dbt

The analytical Gold layer is managed with dbt Core.

**Unity Catalog:** `atliq`

**Schemas:**

```text
atliq.bronze
atliq.silver
atliq.gold
atliq.ci
atliq.fabric_gold
```

`dim_date` provides the calendar dimension used for time-based reporting and for joining order dates into the reporting model.

**Gold model grain:** ⭐ one row per order item

| Layer | Models |
| --- | --- |
| **Dimensions** | `dim_customer`, `dim_product`, `dim_date` |
| **Facts** | `fact_sales`, `fact_marketing_spend` |
| **Intermediate** | `int_sales_enriched` |
| **Staging** | `stg_customers`, `stg_marketing_spend`, `stg_order_items`, `stg_orders`, `stg_payments`, `stg_products`, `stg_supplier_price_list` |

Staging and intermediate models are materialized as **views** in `atliq.gold`; analytical marts are materialized as **tables**.

> 💡 **Revenue definition:** Gross revenue **excludes Cancelled orders**. Returned activity is retained separately via order status so it can be analyzed without silently deflating or inflating the sales measure.

## ✅ dbt Validation

<div align="center">

| Metric | Result |
| --- | --- |
| **Models** | 13 / 13 passed |
| **Tests** | 18 / 18 passed |
| **Sources** | 7 |
| **Warnings** | 0 |
| **Errors** | 0 |
| **Skipped** | 0 |

</div>

```text
$ dbt run
13 of 13 models passed

$ dbt test
18 of 18 tests passed — 0 warnings, 0 errors, 0 skipped
```

Test coverage includes:

- 🚫 Not-null checks
- 🔑 Uniqueness checks
- 🔗 Relationship (referential integrity) checks

dbt evidence: `Phase 1 End-to-End Batch Data Engineering_CB/evidence/M4_GOLD_DBT/`

## 🔍 Data Quality & Reconciliation

| Check | Result |
| --- | --- |
| Customer duplicate check | 0 |
| Product duplicate check | 0 |
| Fact order-item uniqueness | 0 |
| Date referential integrity | 0 orphan rows |
| Supplier cost completeness | 798 / 798 |
| **Gross revenue** | **2,126,260.00** |
| Supplier cost (`quantity × unit cost`) | 1,363,271.87 |
| **Gross profit** | **762,988.13** |
| Profit reconciliation difference | 0.00 ✅ |

These checks validate both structural integrity (dbt tests) and the financial calculations used by the analytical model (manual reconciliation).

## 🌙 Nightly Batch & Reliability

The solution is designed as a nightly batch process:

```text
Azure SQL / CSV
      ↓
ADF ingestion
      ↓
ADLS Bronze
      ↓
Databricks Bronze → Silver
      ↓
dbt Gold
      ↓
Fabric reporting
```

Retry-safe design choices:

- 🔁 Same-day Bronze data can be overwritten on retry
- 🔀 Silver fact processing uses MERGE / business-key logic
- 🏗️ Gold models are rebuilt deterministically from curated upstream data

Nightly execution evidence: `Phase 1 End-to-End Batch Data Engineering_CB/evidence/M5_NIGHTLY_SYNC/`

### Idempotency Proof

The complete `pl_master_batch` pipeline was executed **twice** against the same source state. Results below were captured from `atliq.gold.fact_sales` after each full run:

| Metric | Run 1 | Run 2 | Result |
| --- | --- | --- | --- |
| `fact_sales` row count | 798 | 798 | ✅ Match |
| Total `gross_revenue` | 2,126,260.00 | 2,126,260.00 | ✅ Match |
| Total supplier cost of sold units (`quantity × supplier_cost`) | 1,363,271.87 | 1,363,271.87 | ✅ Match |

> **Result:** The end-to-end pipeline produced identical analytical results on both executions — rerunning the same batch does **not** create duplicate sales records or change calculated financial totals, confirming idempotent / retry-safe processing.

Supporting screenshots: `Phase 1 End-to-End Batch Data Engineering_CB/evidence/M5_NIGHTLY_SYNC/`

## 📊 Microsoft Fabric / Power BI

**AtliQ Commerce Executive Sales & Performance Dashboard** — includes executive KPIs and analytical views:

- 📈 Revenue trend
- 🏆 Top products by revenue
- 🏙️ Top cities by revenue
- 🗂️ Revenue by category

The Gold layer is exposed to Microsoft Fabric through the project's Fabric/OneLake integration path. The Fabric semantic model is built over curated Gold data rather than querying the operational Azure SQL database directly.

Dashboard artifacts and evidence: `Phase 1 End-to-End Batch Data Engineering_CB/evidence/M6_FABRIC/` (includes the editable Power BI/Fabric report, PDF export, dashboard screenshot and data-model screenshot).

> 📝 **Not yet implemented:** a customer cohort view (cohort by signup month vs. first/repeat purchase month).

## ⚙️ CI/CD – GitHub Actions

The dbt project is integrated with GitHub Actions.

**Workflow:** `.github/workflows/ci.yml`

**CI process:**

1. ✅ Check out the repository
2. 📦 Install required dbt tooling
3. 🔐 Load Databricks connection values from GitHub Secrets
4. 🏗️ Run the dbt CI build
5. 🧪 Use dbt tests as a data-quality gate

Secrets are never hard-coded in the repository.

**Pipeline audit logging:** The ADF master pipeline includes `Audit Start` and `Audit End` stored-procedure activities. These write execution metadata — pipeline name, run ID, start/end timestamps, status — to the Azure SQL audit table via `[etl].[usp_log_pipeline_audit]`.

CI/CD evidence: `Phase 1 End-to-End Batch Data Engineering_CB/evidence/M7_CICD/`

## 🖼️ Phase 1 Evidence Gallery

**M2 – Azure Data Factory**

![ADF Master Pipeline](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M2_ADF/pl_master_batch.png)
![ADF SQL to ADLS](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M2_ADF/pl_sql_to_adls.png)
![ADF SQL to Raw](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M2_ADF/pl_sql_to_raw.png)

[Open all M2 ADF evidence →](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M2_ADF/)

**M3 – Databricks Silver**

![Databricks Bronze to Silver](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M3_SILVER/01_Bronze_to_Silver.png)

[Open all M3 Silver evidence →](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M3_SILVER/)

**M4 – dbt Gold**

![dbt DAG Lineage](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M4_GOLD_DBT/DAG_Lineage.png)
![dbt Sources](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M4_GOLD_DBT/dbt_Sources.png)
![dbt Test Results](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M4_GOLD_DBT/dbt_test.png)

[Open all M4 Gold/dbt evidence →](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M4_GOLD_DBT/)

**M5 – Nightly Execution**

![Nightly Run](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M5_NIGHTLY_SYNC/Nightly%20Run.png)

[Open all M5 evidence →](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M5_NIGHTLY_SYNC/)

**M6 – Microsoft Fabric**

![AtliQ Commerce Dashboard](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M6_FABRIC/Dashboard.png)
![Fabric Data Model](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M6_FABRIC/Data%20Model.png)

[Open all M6 Fabric evidence →](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M6_FABRIC/)

**M7 – CI/CD**

GitHub Actions screenshots and supporting evidence: [Open all M7 CI/CD evidence →](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/evidence/M7_CICD/)

---

# 🟢 Phase 2 — Real-Time Data Engineering

Phase 2 builds the **speed lane** for AtliQ Commerce: order events are produced from Python, delivered through Kafka, processed using Databricks Structured Streaming, stored in Bronze/Silver/Gold Delta tables, and supported by Apache Airflow for scheduled quality checks, maintenance, and daily summary work.

Location in repo: `Phase 2 - Real Time/`

## Phase 2 Architecture

![Phase 2 Real-Time Architecture](Phase%202%20-%20Real%20Time/architecture/phase2_architecture.png)

**Event Producer → Confluent Kafka → Databricks Structured Streaming → Bronze → Silver → Gold**, with Apache Airflow running the scheduled operational work around the streaming pipeline.

```text
Python Event Producer
        ↓
Confluent Kafka  (atliq.orders.events)
        ↓
Databricks Structured Streaming
        ↓
Bronze → Silver → Gold
        ↓
5-Minute Revenue Windows

Apache Airflow
        ↓
Freshness Check → OPTIMIZE → Daily Summary
```

**Components:**

| Component | Purpose |
| --- | --- |
| Python Event Producer | Generates AtliQ order events |
| Confluent Cloud Kafka | Transports order events |
| Databricks Structured Streaming | Processes the event stream |
| Bronze | Stores raw Kafka records and metadata |
| Silver | Parses, validates, watermarks and de-duplicates events |
| Gold | Calculates 5-minute revenue windows |
| Apache Airflow | Runs freshness checks, OPTIMIZE and daily summary |

**Streaming design at a glance:**

- Kafka topic: `atliq.orders.events`
- Kafka message key: `order_id`
- Bronze table: `atliq.streaming.bronze_order_events`
- Silver table: `atliq.streaming.silver_order_events`
- Gold table: `atliq.streaming.gold_revenue_5min`
- Silver watermark: 10 minutes
- Silver de-duplication key: `event_id`
- Gold aggregation: 5-minute tumbling windows
- Gold business filter: `payment_received`
- Separate checkpoints for Bronze, Silver and Gold

## Kafka

Topic: `atliq.orders.events`

The producer sends order events as JSON:

```text
key   → order_id
value → JSON event
```

Using `order_id` as the Kafka key keeps events for the same order on the same partition.

Producer files:

```text
Phase 2 - Real Time/producer/
├── order_event_producer.py
├── requirements.txt
└── .env.example
```

Real `.env` files containing credentials are excluded from Git.

## Bronze Streaming

Kafka events are ingested into Bronze while preserving raw event information and Kafka metadata.

- Target: `atliq.streaming.bronze_order_events`
- Captured metadata: Kafka key, Kafka value, topic, partition, offset, timestamp
- Checkpoint: `/Volumes/atliq/streaming/checkpoints/bronze`

## Silver Streaming

Bronze events are parsed with an explicit schema and transformed into structured records.

- Target: `atliq.streaming.silver_order_events`
- Transformations: JSON parsing, data type conversion, `event_ts` timestamp conversion, 10-minute watermark, deduplication using `event_id`
- Checkpoint: `/Volumes/atliq/streaming/checkpoints/silver`

The watermark bounds streaming state while tolerating late-arriving events. Deduplication prevents repeated events from being double-counted as separate business events.

## Gold Streaming

Only successful payment events feed revenue aggregation: `event_type = 'payment_received'`.

A **5-minute tumbling window** calculates `orders_paid` and `revenue`.

- Target: `atliq.streaming.gold_revenue_5min`
- Checkpoint: `/Volumes/atliq/streaming/checkpoints/gold`

Because aggregation uses event time and watermarking, completed Gold windows can appear after the corresponding events arrive, once the watermark advances sufficiently.

## Streaming Checkpoints

Each streaming query has its own checkpoint, letting Bronze, Silver and Gold maintain independent processing state and progress:

```text
/Volumes/atliq/streaming/checkpoints/bronze
/Volumes/atliq/streaming/checkpoints/silver
/Volumes/atliq/streaming/checkpoints/gold
```

The Databricks Free Edition/serverless implementation uses `availableNow=True` for the streaming queries as a platform-specific execution adaptation.

## Airflow Operations

DAG: `atliq_streaming_ops` — Schedule: **hourly**

```text
check_fresh_events → optimize_tables → refresh_daily_summary
```

1. **check_fresh_events** — fails when no fresh events are found in the previous 2 hours, with the message `No fresh events found in the last 2 hours`.
2. **optimize_tables** — runs `OPTIMIZE` on the Silver (`atliq.streaming.silver_order_events`) and Gold (`atliq.streaming.gold_revenue_5min`) tables.
3. **refresh_daily_summary** — rebuilds `atliq.streaming.gold_daily_summary` with orders placed, paid, cancelled and revenue.

## Phase 2 Evidence

```text
Phase 2 - Real Time/evidence/
├── successful_airflow_dag_run.png
├── dq_failure_freshness_check.png
└── dq_failure_log.txt
```

**Successful Airflow DAG Run**

![Successful Airflow DAG Run](Phase%202%20-%20Real%20Time/evidence/successful_airflow_dag_run.png)

**Data Quality Failure**

![Data Quality Freshness Failure](Phase%202%20-%20Real%20Time/evidence/dq_failure_freshness_check.png)

The failure scenario was intentionally tested by stopping the producer and triggering the DAG, demonstrating that the freshness quality gate correctly fails when no recent events are available.

Full write-up: [`Phase 2 - Real Time/Phase2_Writeup.md`](Phase%202%20-%20Real%20Time/Phase2_Writeup.md)

## 📡 Phase 2 — Real-Time Project Status

| Component | Status |
| --- | --- |
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

# 🔀 Phase 1 vs. Phase 2 — Two Independent Lanes

Phase 1 and Phase 2 are **intentionally documented, stored and deployed independently**. Phase 2 does not replace or modify the Phase 1 batch pipeline — it demonstrates how a real-time lane can operate alongside an existing batch platform.

| | Phase 1 — Batch | Phase 2 — Real-Time |
| --- | --- | --- |
| **Cadence** | Nightly batch | Continuous / hourly ops |
| **Ingestion** | Azure Data Factory (metadata-driven) | Python producer → Confluent Kafka |
| **Processing engine** | Azure Databricks (batch) | Azure Databricks Structured Streaming |
| **Storage layers** | ADLS Gen2 Bronze → Databricks Silver → dbt Gold | Delta Bronze → Silver → Gold (`atliq.streaming.*`) |
| **Modeling / testing** | dbt Core, Unity Catalog, 13 models / 18 tests | Schema parsing, watermarking, dedup by `event_id` |
| **Reliability approach** | Idempotency proven via repeated full runs | Watermark + separate checkpoints per layer |
| **Ops monitoring** | ADF pipeline audit logging | Apache Airflow hourly DAG (freshness, OPTIMIZE, summary) |
| **Consumption** | Microsoft Fabric / Power BI executive dashboard | `gold_revenue_5min`, `gold_daily_summary` tables |
| **CI/CD** | GitHub Actions gated on dbt tests | N/A (evidence-based validation) |

---

# 📁 Repository Structure

```text
AtliQ-Commerce-End-to-End-Capstone/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── Phase 1 End-to-End Batch Data Engineering_CB/
│   ├── sql/
│   ├── dbt_project/
│   ├── python/
│   ├── evidence/
│   │   ├── M1_OLTP/
│   │   ├── M2_ADF/
│   │   ├── M3_SILVER/
│   │   ├── M4_GOLD_DBT/
│   │   ├── M5_NIGHTLY_SYNC/
│   │   ├── M6_FABRIC/
│   │   └── M7_CICD/
│   ├── atliq_commerce_architecture.svg
│   ├── Cdebasics_AtliQ_End_to_End_Data_Engineering_Project_Report.pdf
│   ├── Cdebasics_AtliQ_End_to_End_Data_Engineering_Project_Report.docx
│   └── README.md
│
├── Phase 2 - Real Time/
│   ├── airflow/
│   ├── architecture/
│   │   └── phase2_architecture.png
│   ├── databricks/
│   ├── evidence/
│   │   ├── dq_failure_freshness_check.png
│   │   ├── dq_failure_log.txt
│   │   └── successful_airflow_dag_run.png
│   ├── producer/
│   │   ├── .env.example
│   │   ├── order_event_producer.py
│   │   └── requirements.txt
│   └── Phase2_Writeup.md
│
├── .gitignore
└── README.md
```

---

# 🧠 Key Engineering Decisions

<details>
<summary><strong>Why separate OLTP and analytics?</strong></summary>
<br>
The operational database is optimized for transactions; the downstream analytical platform is optimized for reporting and aggregation. Separating them protects the application workload from heavy analytical queries.
</details>

<details>
<summary><strong>Why ADLS Gen2?</strong></summary>
<br>
ADLS provides durable cloud storage independent of compute — data remains available even when Databricks compute is stopped.
</details>

<details>
<summary><strong>Why Databricks?</strong></summary>
<br>
Databricks provides scalable distributed processing for Bronze/Silver transformation, enrichment and incremental workloads (batch), and for Structured Streaming (real-time).
</details>

<details>
<summary><strong>Why dbt?</strong></summary>
<br>
dbt provides SQL-based analytical modeling, dependency management, testing and documentation in one workflow.
</details>

<details>
<summary><strong>Why ADF?</strong></summary>
<br>
ADF provides managed ingestion and orchestration between the operational source, storage and transformation layers. The control-table-driven design makes the pipeline reusable — new source tables are onboarded through metadata rather than a new pipeline per table.
</details>

<details>
<summary><strong>Why GitHub Actions?</strong></summary>
<br>
GitHub Actions provides automated CI validation so dbt changes are tested consistently before merge.
</details>

<details>
<summary><strong>Why not directly join sales and marketing by date?</strong></summary>
<br>
Because several campaigns or channels can exist on the same date. Without a sale-to-campaign attribution key, a direct join can duplicate sales and produce incorrect metrics — so the two are modeled as separate facts instead.
</details>

<details>
<summary><strong>Why run Phase 2 as a separate lane instead of replacing Phase 1?</strong></summary>
<br>
Nightly batch remains the right fit for governed, dimensionally-modeled executive reporting, while streaming answers "what's happening right now" questions that a nightly cadence can't. Keeping them independent means Phase 2 can be developed, tested and evolved without risk to the production batch platform.
</details>

---

# 🛠️ Issues Faced & Resolutions

| Issue | Resolution |
| --- | --- |
| **PowerShell execution policy** blocked local dbt activation/scripts | Used a process-scoped PowerShell execution-policy change |
| **dbt–Databricks connection** initially misconfigured | Corrected environment variables and Databricks host config; validated with `dbt debug` |
| **Credential exposure** — a Databricks token was accidentally exposed during troubleshooting | Credential was rotated immediately; secrets now live only in environment variables and GitHub Secrets |
| **GitHub Actions authentication failure** due to a stale secret | Updated the GitHub secret; CI passed on the next run |
| **Catalog naming** — inconsistent references to the `atliq` Unity Catalog | Standardized on `atliq` everywhere to avoid object-not-found errors |

---

# 🔒 Security

**Never commit:**

- ❌ Databricks tokens
- ❌ Passwords
- ❌ `.env` files containing secrets
- ❌ Connection strings containing credentials

✅ Use environment variables, GitHub Secrets, or an appropriate secret-management service.

---

# 💻 Local dbt Commands

From the dbt project directory:

```powershell
cd "Phase 1 End-to-End Batch Data Engineering_CB/dbt_project"
dbt debug
dbt run
dbt test
dbt build
```

Connection credentials should be supplied securely through environment variables — never hard-coded in `profiles.yml`.

---

# 📚 Project Documentation

The repository contains both PDF and Word versions of the detailed Phase 1 project documentation, covering implementation, architecture, design decisions, validation, troubleshooting and learning outcomes. Phase 2 has its own dedicated write-up.

- 📄 [**View Phase 1 Project Report (PDF)**](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/Cdebasics_AtliQ_End_to_End_Data_Engineering_Project_Report.pdf)
- 📝 [**Download Phase 1 Project Report (DOCX)**](Phase%201%20End-to-End%20Batch%20Data%20Engineering_CB/Cdebasics_AtliQ_End_to_End_Data_Engineering_Project_Report.docx)
- 📄 [**Phase 2 Write-up**](Phase%202%20-%20Real%20Time/Phase2_Writeup.md)

---

# 🎓 Project Outcome

This project demonstrates an end-to-end data engineering workflow covering:

- ✅ OLTP data modeling · Incremental ingestion · ADLS Gen2 data lake storage
- ✅ Bronze / Silver / Gold medallion architecture · Azure Databricks transformation · Unity Catalog
- ✅ Dimensional modeling · dbt transformation and testing · Data-quality validation
- ✅ Nightly orchestration · Retry/idempotency-oriented processing
- ✅ Microsoft Fabric analytics · Git version control · GitHub Actions CI/CD
- ✅ Kafka + Structured Streaming · Watermarking & deduplication · Airflow ops monitoring
- ✅ Technical documentation and implementation evidence throughout both phases

---

<div align="center">

# 👤 Author

**Jaideep Gupta**

*Data Engineering / Analytics Engineering Portfolio Project*

**Technologies:** Azure SQL · Azure Data Factory · ADLS Gen2 · Databricks · Unity Catalog · dbt · Microsoft Fabric · Power BI · Kafka · Airflow · GitHub Actions

</div>
