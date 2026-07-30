# Muller Manufacturing Data Platform

An end-to-end data and AI project built on Azure and Databricks. It combines
historical business data with live IoT sensor streaming, and adds a small
LLM-based sentiment step on top, all wired into a simple CI/CD pipeline.

---

## 1. What This Project Does

This project simulates a dairy / manufacturing operation and answers a few
practical questions in one place:

- Which plants and lines have higher defect and scrap rates?
- Which customers have late or returned shipments?
- What does live sensor data say about equipment health?
- What do quality audit notes say beyond structured outcome labels?

To do that, it:

- Generates synthetic data for plants, lines, SKUs, customers, batches,
  shipments, and audit logs.
- Uploads historical business data to Azure Data Lake Storage Gen2.
- Reads sensor data in two forms: a stored JSON snapshot and a live Event Hubs stream.
- Ingests everything into Databricks using a Bronze → Silver → Gold layout.
- Adds a small LLM step to classify audit notes as positive, neutral, or negative.
- Shows the results in a Databricks dashboard and through Genie.

---

## 2. Architecture

The platform follows a simple Medallion-style layout on Databricks with Unity
Catalog:

- **Bronze**: raw tables loaded from files and live streaming input.
- **Silver**: cleaned, typed, deduplicated tables.
- **Gold**: business summary tables plus one sentiment table from the LLM step.

There are two main ingestion paths:

- Historical business files from Azure Data Lake Storage Gen2.
- Live sensor events from Azure Event Hubs.

Sensor data is handled in two forms:

- `sensor_readings`: a batch JSON snapshot read from storage
- `sensor_readings_stream`: live sensor events read from Azure Event Hubs

Both use the same sensor schema and are later combined in Silver into
`fact_sensor_readings`, with a `data_source` column showing whether the row came
from the batch snapshot or the live stream.

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
Live sensor events → Event Hubs → stream_bronze_ingest.py → sensor_readings_stream → BRONZE
```

This design makes it easy to compare stored sensor data and live sensor data in
one reporting model.

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
│   └── azure_settings.py          # catalog, schema names, paths, secrets
├── src/
│   ├── data_generation/
│   │   ├── generate_dimensions.py  # plants, lines, SKUs, customers
│   │   ├── generate_historical.py  # batches, shipments, audits
│   │   └── generate_streaming.py   # sensor snapshot data
│   ├── azure_sync/
│   │   ├── upload_to_adls.py       # upload local files to ADLS
│   │   └── eventhub_producer.py    # send sensor events into Event Hubs
│   ├── pipeline/
│   │   ├── bronze_ingest.py        # ADLS → Bronze tables
│   │   ├── stream_bronze_ingest.py # Event Hubs → Bronze stream table
│   │   ├── silver_transform.py     # Bronze → Silver
│   │   ├── gold_aggregate.py       # Silver → Gold metrics
│   │   └── sentiment_analysis.py   # Silver audits → Gold sentiment
│   └── utils/
│       └── logger.py               # logging helper
├── notebooks/
│   ├── run_pipeline                # runs Bronze→Silver→Gold→sentiment
│   └── run_streaming               # runs streaming ingestion
├── tests/
│   ├── test_bronze_ingest.py
│   ├── test_silver_transform.py
│   ├── test_gold_aggregate.py
│   ├── test_sentiment_analysis.py
│   └── test_upload_to_adls.py
├── .github/
│   └── workflows/
│       └── deploy.yml
└── requirements.txt
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

There are two Databricks jobs:

**Job 1 — `muller_medallion_pipeline`**

- runs Bronze, Silver, Gold, and sentiment steps in order
- runs on a wider schedule, such as hourly or daily

**Job 2 — `sensor_streaming_ingest`**

- reads live events from Event Hubs
- writes them into `sensor_readings_stream`
- uses `trigger(availableNow=True)`
- runs more often, for example every few minutes

This setup works well on serverless compute without needing an always-running stream.

---

## 8. Dashboard

The Databricks AI/BI dashboard has four main views:

1. **Executive** — high-level KPIs like yield, defects, delivery, and sentiment
2. **Production & Quality** — plant and line performance, audit outcomes, and sentiment
3. **Customer** — customer delivery and return metrics
4. **Equipment Health** — sensor anomaly and health metrics, split by stored vs live data

Genie is enabled on top of the Gold tables so users can ask simple questions
without writing SQL.

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

### 10.3 Generate and upload data

```bash
python -m src.data_generation.generate_dimensions
python -m src.data_generation.generate_historical
python -m src.data_generation.generate_streaming
python -m src.azure_sync.upload_to_adls
```

### 10.4 Start the sensor producer

```bash
python -m src.azure_sync.eventhub_producer --mode continuous
```

---

## 11. Next Steps

Possible improvements:

- Add a Gold table for time-series trends
- Move the sensor producer to Azure Functions or a container
- Add more data quality checks between Bronze and Silver
- Add alerts for anomaly spikes or negative audit sentiment
- Add a small evaluation notebook for sentiment results