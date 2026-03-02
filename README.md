# 🏗️ OLTP → OLAP Data Engineering Pipeline  
### E-Commerce Analytics with Airflow

This project implements an end-to-end **Data Engineering pipeline** that transforms transactional (OLTP) data into an analytical (OLAP) data warehouse, fully orchestrated and scheduled using Apache Airflow.

The goal is to simulate a real-world production-ready data platform where data ingestion, transformation, validation, and orchestration are automated and reproducible.

---

## 📊 Architecture Overview


Raw CSV Data
↓
OLTP Layer (PostgreSQL)
↓
SQL Transformations
↓
Star Schema (OLAP)
↓
Data Quality Gate
↓
Scheduled Airflow Pipeline


---

## 🧱 Tech Stack

- **Python** — Data ingestion
- **PostgreSQL** — OLTP & OLAP storage
- **Apache Airflow** — Orchestration & scheduling
- **Docker & Docker Compose** — Containerized infrastructure
- **SQL** — Transformations & modeling

---

## 📂 Project Structure


E-Commerce/
│
├── dags/
│ └── olist_oltp_to_olap.py
│
├── raw/
│ └── Olist E-commerce datasets (CSV)
│
├── src/
│ └── load_oltp.py
│
├── sql/
│ ├── 02_olap/
│ │ ├── 01_create_analytics_tables.sql
│ │ ├── 02_populate_dims.sql
│ │ └── 03_populate_fact.sql
│ │
│ └── 04_tests/
│ ├── tests.sql
│ └── quality_gate.sql
│
├── docker-compose.yml
└── README.md


---

## 🔄 Pipeline Workflow (Airflow DAG)

The DAG `olist_oltp_to_olap` executes the following steps:

### 1️⃣ Load OLTP
- Ingest CSV files into the `oltp` schema.
- Simulates operational database ingestion.

### 2️⃣ Create OLAP Schema
- Creates analytical schema `analytics`.
- Defines Star Schema tables:
  - `fact_sales`
  - `dim_customer`
  - `dim_product`
  - `dim_date`

### 3️⃣ Populate Dimensions
- Builds dimensional tables from transactional data.

### 4️⃣ Populate Fact Table
- Generates analytical sales facts.
- Computes revenue metrics.

### 5️⃣ Data Quality Gate ✅
Automated validations ensure warehouse integrity:

- Fact table is not empty
- No NULL foreign keys
- No duplicated order items
- Revenue values are valid
- Dimensions contain data

If any check fails → the pipeline stops automatically.

### 6️⃣ Analytical Tests
Additional SQL validation queries verify consistency.

---

## ⏱️ Scheduling & Orchestration

The pipeline runs automatically:


Schedule: Daily at 03:00 AM


Airflow manages:

- Task dependencies
- Execution history
- Retries on failure
- Failure logging
- Manual and scheduled runs

---

## 🐳 Containerized Infrastructure

All services run inside Docker:

- PostgreSQL database
- Airflow Webserver
- Airflow Scheduler

Start the full environment:

```bash
docker compose up -d

Access Airflow UI:

http://localhost:8080
📐 Data Modeling

The OLAP layer follows a Star Schema optimized for analytics:

           dim_customer
                 |
dim_date — fact_sales — dim_product

This enables queries such as:

Revenue by month

Sales by product category

Customer purchasing patterns

✅ Data Engineering Concepts Implemented

✔ OLTP → OLAP transformation
✔ Dimensional modeling (Star Schema)
✔ Workflow orchestration with Airflow
✔ Automated scheduling
✔ Data quality validation gates
✔ Idempotent SQL transformations
✔ Containerized infrastructure

🚀 Motivation

Separating transactional workloads from analytical systems is a fundamental principle in Data Engineering.

This project demonstrates how raw operational data can be transformed into a structured analytical warehouse, validated automatically, and orchestrated using modern tooling.

🔮 Future Improvements

Incremental loading strategy

Alert integrations (Slack / Email)

BI dashboard integration

Cloud deployment

Monitoring & observability extensions
