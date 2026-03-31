# 📡 Telecom Data Platform

This project demonstrates an **end-to-end data engineering pipeline** combining:

* Batch processing
* Real-time streaming (Kafka)
* ELT transformations (dbt)
* Orchestration (Airflow)

---

## 🛠 Tech Stack

* **Python** – ETL scripts & Airflow DAGs
* **PostgreSQL** – OLTP and raw data storage
* **Kafka** – Real-time event streaming
* **dbt** – Transformations, incremental models, and tests
* **Airflow** – Orchestration of batch & streaming pipelines
* **Docker** – Containerized infrastructure

---

## 📁 Project Structure

| Folder       | Purpose                                            |
| ------------ | -------------------------------------------------- |
| `ingestion/` | Data ingestion scripts (batch + streaming)         |
| `dbt/`       | dbt transformations (staging, intermediate, marts) |
| `airflow/`   | DAGs, plugins, and configuration                   |
| `docker/`    | Dockerfiles and Compose configuration              |

---

## 🚦 Pipeline Status & Architecture

### Architecture Overview

```
┌─────────────┐
│  Source DB  │  ← optional initial batch ingestion
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Batch ETL   │  ← Airflow DAG task: batch_ingestion
│ raw_user_activity table
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Kafka       │  ← Producer generates events
│ Topic:      │
│ user_activity
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Kafka       │  ← Consumer reads from topic
│ Consumer    │
│ Writes to   │
│ raw_user_activity table
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ dbt Models  │
│ staging →   │
│ intermediate → marts
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ dbt Tests   │
│ Data quality│
└─────────────┘
```

---

## 🧩 Workflow (Airflow DAG)

**DAG Name:** `telecom_pipeline`

**Execution Order:**

```
batch_ingestion → kafka_producer → kafka_consumer → dbt_run → dbt_test
```

### Tasks:

1. **Batch Ingestion (`batch_ingestion`)**

    * Pulls initial data from source or CSV
    * Inserts into `raw_user_activity` table in PostgreSQL

2. **Kafka Producer (`kafka_producer`)**

    * Generates synthetic user events (calls, SMS, internet usage)
    * Publishes messages to Kafka topic `user_activity`

3. **Kafka Consumer (`kafka_consumer`)**

    * Reads messages from Kafka topic `user_activity`
    * Inserts events into `raw_user_activity` table in PostgreSQL

4. **DBT Transformations (`dbt_run`)**

    * **Staging:** `stg_user_activity`
    * **Intermediate:** `int_user_activity`
    * **Marts:** `mrt_user_metrics`

5. **DBT Tests (`dbt_test`)**

    * Validates **not-null** and **unique** constraints
    * Ensures transformed data quality

---

## ⚙️ Notes & Best Practices

### Database Setup

* PostgreSQL schemas: `raw`, `staging`, `intermediate`, `marts`
* Airflow metadata DB uses a separate Postgres instance: `airflow_postgres`

### Kafka Setup

* Topic: `user_activity`
* Zookeeper & Kafka run in Docker alongside Airflow

### Environment Variables

* Configured in `.env` file
* Loaded in DAG and ETL scripts via `python-dotenv`

### Persistence

* PostgreSQL volumes: `pgdata`, `airflow_pgdata` ensure schemas survive Docker restarts
* **Do not run `docker-compose down -v`** if you want to preserve data