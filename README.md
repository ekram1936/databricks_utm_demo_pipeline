# Muller Manufacturing Data Platform

An end-to-end data and AI project built on Azure and Databricks. It combines historical business data with live IoT sensor streaming, adds a small LLM-based sentiment step, and uses GitHub Actions plus Databricks Asset Bundles for deployment.

---

## 1. What This Project Does

This project simulates a dairy / manufacturing operation and answers a few practical questions in one place:

- Which plants and lines have higher defect and scrap rates?
- Which customers have late or returned shipments?
- What does live sensor data say about equipment health?
- What do quality audit notes say beyond structured outcome labels?

To do that, it:

- Generates synthetic data for plants, lines, SKUs, customers, batches, shipments, and audit logs.
- Uploads historical business data to Azure Data Lake Storage Gen2.
- Reads sensor data in two forms: a stored JSON snapshot and a live Event Hubs stream.
- Ingests everything into Databricks using a Bronze → Silver → Gold layout.
- Adds a small LLM step to classify audit notes as positive, neutral, or negative.
- Shows the results in a Databricks dashboard and through Genie.

---

## 2. Architecture

The platform follows a simple Medallion-style layout on Databricks with Unity Catalog:

- **Bronze**: raw tables loaded from files and live streaming input.
- **Silver**: cleaned, typed, deduplicated tables.
- **Gold**: business summary tables plus one sentiment table from the LLM step.

There are two main ingestion paths:

- Historical business files from Azure Data Lake Storage Gen2.
- Live sensor events from Azure Event Hubs.

Sensor data is handled in two forms:

- `sensor_readings`: a batch JSON snapshot read from storage
- `sensor_readings_stream`: live sensor events read from Azure Event Hubs

Both use the same sensor schema and are later combined in Silver into `fact_sensor_readings`, with a `data_source` column showing whether the row came from the batch snapshot or the live stream.

```text
Historical files → ADLS → bronze_ingest.py → BRONZE
                                         │
                                         ▼
                               silver_transform.py → SILVER
                                         │
                                         ▼
                               gold_aggregate.py → GOLD metrics
                                         │
                                         ▼
                             sentiment_analysis.py → GOLD sentiment

Sensor snapshot → ADLS JSON → sensor_readings → BRONZE
Live sensor events → Event Hubs → streaming ingestion workflow → sensor_readings_stream → BRONZE
```

This design makes it easy to compare stored sensor data and live sensor data in one reporting model.

---

## 3. Tech Stack

| Part | Tool |
|---|---|
| Cloud | Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Streaming | Azure Event Hubs |
| Compute | Databricks serverless |
| Table format | Delta Lake |
| Catalog | Unity Catalog |
| Processing | PySpark + Spark Structured Streaming |
| LLM call | Databricks `ai_query()` |
| Dashboards | Databricks AI/BI + Genie |
| CI/CD | GitHub Actions + Databricks Asset Bundles |
| Tests | pytest |

---

## 4. Repository Layout

```text
databricks_utm_demo_pipeline/
├── config/
│   ├── __init__.py
│   ├── azure_settings.py          # Azure, catalog, schema, and environment settings
│   └── settings.py                # local and project configuration
├── conftest.py                    # pytest shared fixtures/config
├── data/
│   └── raw/
│       ├── dim/
│       │   ├── dim_lines.csv
│       │   ├── dim_plants.csv
│       │   ├── dim_retail_customers.csv
│       │   └── dim_sku_catalog.csv
│       ├── historical/
│       │   ├── historical_production_batches.csv
│       │   ├── historical_quality_audit_logs.csv
│       │   └── historical_shipments.csv
│       └── streaming/
│           └── sensor_readings_*.jsonl
├── databricks.yml                 # Databricks Asset Bundle configuration
├── LICENSE
├── main.py                        # local entry point for generating synthetic raw data
├── README.md
├── requirements.txt
├── resources/
│   ├── main_pipeline_job.yml              # Databricks job definition
│   ├── manufacturing_bronze.pipeline.yml  # Bronze pipeline resource
│   ├── manufacturing_gold.pipeline.yml    # Gold pipeline resource
│   └── manufacturing_silver.pipeline.yml  # Silver pipeline resource
├── src/
│   ├── __init__.py
│   ├── azure_sync/
│   │   ├── __init__.py
│   │   ├── eventhub_producer.py    # send live sensor events to Event Hubs
│   │   └── upload_to_adls.py       # upload generated data to ADLS
│   ├── data_generation/
│   │   ├── __init__.py
│   │   ├── generate_dimensions.py  # plants, lines, SKUs, customers
│   │   ├── generate_historical.py  # batches, shipments, audit logs
│   │   └── generate_streaming.py   # sensor snapshot / stream seed data
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── bronze_ingest.py        # ADLS / raw input → Bronze
│   │   ├── gold_aggregate.py       # Silver → Gold business summaries
│   │   ├── sentiment_analysis.py   # audit-note sentiment enrichment
│   │   └── silver_transform.py     # Bronze → Silver cleaning and modeling
│   └── utils/
│       ├── __init__.py
│       └── logger.py               # logging helper
└── tests/
    ├── __init__.py
    ├── test_bronze_ingest.py
    ├── test_gold_aggregate.py
    ├── test_sentiment_analysis.py
    ├── test_silver_transform.py
    └── test_upload_to_adls.py
```

---

## 5. Data Model

### 5.1 Bronze (`utm_demo_catalog.bronze`)

Bronze tables are close to the raw input.

| Table | What it holds |
|---|---|
| `dim_plants` | plant info |
| `dim_lines` | production line info |
| `dim_sku_catalog` | SKU info |
| `dim_retail_customers` | customer info |
| `historical_production_batches` | batch records |
| `historical_shipments` | shipment records |
| `historical_quality_audit_logs` | audit text and outcomes |
| `sensor_readings` | sensor batch snapshot |
| `sensor_readings_stream` | live sensor events from Event Hubs |

### 5.2 Silver (`utm_demo_catalog.silver`)

Silver applies data cleaning and builds fact tables.

| Table | Notes |
|---|---|
| `dim_plants`, `dim_lines`, `dim_sku_catalog`, `dim_retail_customers` | cleaned dimensions |
| `fact_production_batches` | batches with yield and defect info |
| `fact_shipments` | shipments with delivery status |
| `fact_quality_audits` | audits with one row per `audit_id` |
| `fact_sensor_readings` | combined sensor table from batch snapshot + live stream, tagged with `data_source` |

### 5.3 Gold (`utm_demo_catalog.gold`)

Gold tables are used by the dashboard and Genie.

| Table | Notes |
|---|---|
| `d_production_summary` | yield and defect KPIs by plant and line |
| `d_retailer_360` | delivery and return metrics by customer |
| `d_quality_audit_summary` | audit outcome counts by plant |
| `d_audit_sentiment` | sentiment and root-cause info for audit notes |
| `d_sensor_health_summary` | anomaly and health metrics by line and `data_source` |

---

## 6. Sentiment Step

The AI part of this project is small and focused.

- Input: `silver.fact_quality_audits`
- Tool: Databricks `ai_query()`
- Output: `gold.d_audit_sentiment`

For each audit note, the pipeline:

- assigns a sentiment label: **Positive**, **Neutral**, or **Negative**
- adds a one-word root cause for negative cases
- writes the result to a Gold table

The logic is designed so that:

- the target table is created if it does not already exist
- only new audits are sent for scoring, based on `audit_id`

This keeps the LLM step simple and avoids processing the same audit more than once.

---

## 7. Jobs and Scheduling

The core pipeline is deployed and orchestrated through Databricks Asset Bundles:

**Job — `muller_medallion_pipeline`**

- orchestrates the Bronze, Silver, Gold, and sentiment steps in order
- is defined in `resources/main_pipeline_job.yml` and deployed via `databricks.yml`
- can be scheduled in Databricks to run on a cadence such as hourly or daily

For streaming, this project uses a lightweight producer and a serverless pipeline:

- `src.azure_sync.eventhub_producer` sends sensor events into Azure Event Hubs
- a serverless Lakeflow / pipeline definition ingests events into `sensor_readings_stream`
- the pipeline can be triggered on demand or on a schedule without requiring an always-on cluster

This setup is designed for serverless compute, so both batch and streaming paths can run without long-lived clusters.

---

## 8. Dashboard

The Databricks AI/BI dashboard has four main views:

1. **Executive** — high-level KPIs like yield, defects, delivery, and sentiment
2. **Production & Quality** — plant and line performance, audit outcomes, and sentiment
3. **Customer** — customer delivery and return metrics
4. **Equipment Health** — sensor anomaly and health metrics, split by stored vs live data

Genie is enabled on top of the Gold tables so users can ask simple questions without writing SQL.

---

## 9. Tests and CI/CD

### 9.1 Tests

The `tests/` folder contains pytest tests for:

- Bronze ingestion
- Silver transformations
- Gold aggregates
- sentiment logic
- ADLS upload logic

### 9.2 GitHub Actions

`.github/workflows/deploy.yml` defines a simple pipeline:

```text
test → validate → deploy
```

- `test`: install dependencies and run tests
- `validate`: run `databricks bundle validate -t dev`
- `deploy`: deploy the bundle and trigger the main job

If tests fail, deployment does not continue.

---

## 10. How to Run

### 10.1 Prerequisites

- Azure subscription with ADLS, Event Hubs, and Databricks
- Python 3.10
- Databricks CLI configured
- Databricks secret scope for the Event Hubs connection string

```bash
databricks secrets create-scope eventhub-secrets
databricks secrets put-secret eventhub-secrets connection-string
```

### 10.2 Local setup

```bash
git clone <repo-url>
cd databricks_utm_demo_pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export EVENT_HUB_CONNECTION_STR='<your-event-hubs-connection-string>'
export EVENT_HUB_NAME='sensor-telemetry'
```

### 10.3 Generate synthetic data and upload to ADLS

```bash
python main.py
python -m src.azure_sync.upload_to_adls
```

### 10.4 Start the sensor producer

```bash
python -m src.azure_sync.eventhub_producer --mode continuous
```

---

## 11. Development Cost

This project was developed on Azure for Students using Azure Databricks serverless resources and GitHub Actions for CI/CD. During development, the main cost came from Databricks serverless compute, while GitHub Actions usage remained within the included free tier.

### 11.1 Azure / Databricks cost breakdown

| Component | Cost | Description |
|---|---:|---|
| Premium Automated Serverless Compute DBU | €19.15 | Bronze, Silver, and Gold pipeline/job execution on serverless compute |
| Premium Interactive Serverless Compute DBU | €12.19 | Notebook-based development and debugging |
| Premium Serverless SQL DBU | €12.13 | SQL validation, exploration, and dashboard checks |
| Premium Serverless Realtime Inferencing DBU | €5.48 | Inference and AI-related serving usage |
| Premium Databricks Storage Unit DSU | €0.06 | Databricks-managed storage overhead |
| NAT Gateway | €0.02 | Minor network processing cost |
| Bandwidth | €0.00 | No meaningful bandwidth cost |

These costs were driven mainly by compute usage time rather than data size, since Databricks charges by DBU consumption for active resources.

### 11.2 GitHub Actions cost

GitHub Actions usage for the repository in July 2026:

| Metric | Value |
|---|---:|
| Total minutes | 61 |
| Workflow runs | 13 |
| Job runs | 22 |
| Workflow | `deploy.yml` |
| Runner type | GitHub-hosted |
| Runtime OS | Linux |
| Gross amount | $0.37 |
| Billed amount | $0.00 |

GitHub-hosted Linux runners are priced at $0.006 per minute, which matches the observed gross cost. The billed amount remained $0 because usage stayed within the included free allowance.

### 11.3 Summary

The major development expense for this project was Azure Databricks serverless compute. GitHub Actions CI/CD cost was effectively negligible for the month measured.

---

## 12. Next Steps

Possible improvements:

- Add a Gold table for time-series trends
- Move the sensor producer to Azure Functions or a container
- Add more data quality checks between Bronze and Silver
- Add alerts for anomaly spikes or negative audit sentiment
- Add a small evaluation notebook for sentiment results
