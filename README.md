# 🏗️ Databricks Lakehouse — Upsell Pipeline

A production-grade **Medallion Architecture** data pipeline built on Databricks, designed to process loyalty/upsell program data through Bronze, Silver, and Gold layers using **CDC (Change Data Capture)**, **CDF (Change Data Feed)**, and Delta Lake. The pipeline is orchestrated via a Databricks Workflow and deployed via GitOps using the Databricks Jobs API.

---

## 📁 Repository Structure

```
databricks_lakehouse/
├── src/
│   ├── bronze/
│   │   ├── ingestion.ipynb                          # Bronze ingestion notebook (parameterized)
│   │   ├── customers.json                           # Schema definition for customers
│   │   ├── transactions.json                        # Schema definition for transactions
│   │   └── transactions_product.json                # Schema definition for transaction cart items
│   ├── silver/
│   │   ├── ingestion.ipynb                          # Silver ingestion notebook (parameterized)
│   │   ├── customers.dbquery.ipynb                  # CDF transformation query for customers
│   │   ├── transactions.dbquery.ipynb               # CDF transformation query for transactions
│   │   ├── transactions_product.dbquery.ipynb       # CDF transformation query for cart items
│   │   └── products.dbquery.ipynb                   # CDF transformation query for products
│   ├── gold/
│   │   ├── ingestion.ipynb                          # Gold ingestion notebook (parameterized)
│   │   ├── daily_report.sql                         # Daily aggregated report SQL
│   │   ├── montly_report.sql                        # Monthly rolling window report SQL
│   │   └── churn_report.sql                         # Customer churn rate report SQL
│   └── lib/
│       ├── ingestors.py                             # Core ingestor classes (Ingestor, IngestorCDC, IngestorCDF, IngestorCubo)
│       └── utils.py                                 # Helper utilities (schema import, query manipulation, date range)
├── workflows/
│   ├── main.py                                      # Script to deploy/reset all workflow definitions via Databricks API
│   └── upsell.json                                  # Full Databricks Job definition for the Upsell pipeline
├── pyproject.toml                                   # Python project metadata and dependencies
├── uv.lock                                          # Locked dependency file (managed by uv)
└── .gitignore
```

---

## 🧱 Architecture Overview — Medallion

The project follows the **Medallion (Bronze → Silver → Gold)** lakehouse architecture:

```
Source Systems (CDC/Full Load)
         │
         ▼
  ┌─────────────┐
  │   BRONZE    │  Raw data, CDC ingestion via AutoLoader (cloudFiles)
  │  Delta Lake │  Tables: customers, transactions, transactions_product
  └──────┬──────┘
         │  CDF (Change Data Feed) streams
         ▼
  ┌─────────────┐
  │   SILVER    │  Cleansed, renamed, type-cast data
  │  Delta Lake │  Tables: customers, transactions, transactions_product, products
  └──────┬──────┘
         │  SQL batch queries
         ▼
  ┌─────────────┐
  │    GOLD     │  Business-ready aggregations and KPIs
  │  Delta Lake │  Tables: daily_report, monthly_report, churn_report
  └─────────────┘
```

---

## ⚙️ Tech Stack

| Component | Technology |
|-----------|------------|
| Compute | Databricks |
| Storage format | Delta Lake |
| Streaming ingestion | Apache Spark Structured Streaming + AutoLoader (`cloudFiles`) |
| CDC merge | Delta `MERGE INTO` via DeltaTable API |
| CDF (Silver) | Delta Change Data Feed (`readChangeFeed`) |
| Language | Python 3.13+ · PySpark · SQL |
| Job orchestration | Databricks Workflows (Jobs API 2.1) |
| Deployment | Databricks Jobs REST API |
| Source control | GitHub (GitOps — notebooks sourced from `main` branch) |
| Dependency management | `uv` + `pyproject.toml` |
| Python dependencies | `python-dotenv`, `requests` |
| Scheduling | Quartz cron (`00 00 7 * * ?` — daily at 07:00 America/Sao_Paulo) |

---

## 🔄 Workflow: Upsell Pipeline

The workflow is defined in `workflows/upsell.json` and deployed via `workflows/main.py`. It consists of **10 tasks** that execute in a DAG with strict `ALL_SUCCESS` dependencies.

### Schedule

| Setting | Value |
|---------|-------|
| Cron expression | `00 00 7 * * ?` |
| Timezone | `America/Sao_Paulo` |
| Pause status | `UNPAUSED` |
| Max concurrent runs | `1` |

### Task DAG

```
bronze_customers ──────────────────────────────┐
                                               ▼
                                        silver_customers ───────────────────────┐
                                                                                │
bronze_transactions ───────────────────────────┐                                │
                                               ▼                                │
                                        silver_transactions ──────┐             │
                                                                  ├──► gold_daily_report
bronze_transactions_product ─────────────────┬►silver_transactions_product    ──┤
                                             │                    ├──► gold_monthly_report
                                             └►silver_products ───┘             │
                                                                  └───► gold_churn_report
```

> All tasks run `ALL_SUCCESS` — if any upstream task fails, its dependents are skipped.


## 🚀 Workflow Deployment (`workflows/main.py`)

The `main.py` script reads every `.json` file in the `workflows/` directory and calls the Databricks **Jobs Reset API** (`POST /api/2.1/jobs/reset`) to update each job's definition in place without creating a new job.

```python
# Environment variables required
DATABRICKS_HOST = "..."   # e.g. adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN = "..."  # Personal Access Token or Service Principal token
```
This enables **GitOps-style deployment**: change the JSON, run `main.py`, and the live Databricks job is updated immediately.

All notebooks are sourced directly from GitHub (`"source": "GIT"`) using the repository's `main` branch, meaning no manual notebook uploads are required — Databricks pulls the latest code on each run.

---

## 🗂️ Unity Catalog Layout

```
bronze.upsell.customers
bronze.upsell.transactions
bronze.upsell.transactions_product

silver.upsell.customers
silver.upsell.transactions
silver.upsell.transactions_product
silver.upsell.products

gold.upsell.daily_report
gold.upsell.monthly_report
gold.upsell.churn_report
```

Volume paths for raw data ingestion:
```
/Volumes/raw/upsell/full_load/{tablename}/     ← Initial full-load Parquet files
/Volumes/raw/upsell/cdc/{tablename}/            ← Ongoing CDC files (AutoLoader)
/Volumes/raw/upsell/cdc/{tablename}_checkpoint/ ← Streaming checkpoints
```

---
