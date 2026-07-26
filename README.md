# Muller Manufacturing Data Platform

An end-to-end data engineering project built on Azure and Databricks that combines
batch historical data with live IoT sensor streaming to power analytics and an
AI-assisted management dashboard for a simulated dairy/manufacturing operation.

---

## 1. Problem Statement

Manufacturing plants generate two very different kinds of data that are traditionally
hard to unify:

1. **Historical/operational records** — production batches, shipments, quality audit
   logs, and master data (plants, lines, SKUs, customers) — usually sitting in files
   or business systems, updated periodically.
2. **Live equipment telemetry** — temperature, pressure, vibration, fill volume, and
   machine status readings streaming continuously off the factory floor.

Without a unified pipeline, plant managers can't answer basic operational questions
in one place:
- Which plant has the highest defect rate?
- Which retail customers are at risk of missed deliveries?
- Are any production lines showing abnormal equipment behavior right now?
- What do our quality audit notes actually say, in aggregate?

This project builds a single pipeline and dashboard that answers all of the above,
combining **batch** and **real-time streaming** data sources into one governed,
queryable platform — plus a natural-language "ask a question" interface for
non-technical managers.

---

## 2. Solution Overview

A **Medallion Architecture** (Bronze → Silver → Gold) built on Databricks and Unity
Catalog, fed by two parallel ingestion paths:

```
                     ┌─────────────────────────┐
                     │   Synthetic Data Gen     │
                     │ (plants, lines, SKUs,    │
                     │  customers, historical   │
                     │  batches/shipments/audits)│
                     └───────────┬──────────────┘
                                 │  CSV / JSONL
                                 ▼
                     ┌─────────────────────────┐
                     │   Azure Data Lake (ADLS  │
                     │   Gen2) — raw file store │
                     └───────────┬──────────────┘
                                 │
   ┌─────────────────┐          │           ┌──────────────────────┐
   │  Local Producer  │          │           │  bronze_ingest.py     │
   │ (eventhub_producer│          │          │  (batch file reader)  │
   │  .py) — simulates │          │          └───────────┬──────────┘
   │  live sensors     │          │                      │
   └────────┬──────────┘          │                      │
            │ sends events        │                      │
            ▼                     │                      ▼
   ┌──────────────────┐           │           ┌──────────────────────┐
   │  Azure Event Hubs │           │           │   BRONZE LAYER        │
   │ (Kafka-compatible)│──────────┼──────────▶│  (Unity Catalog)      │
   └────────┬──────────┘  reads via           │  9 raw tables         │
            │             Kafka protocol       └───────────┬──────────┘
            ▼                                               │
   ┌──────────────────────┐                                 ▼
   │ stream_bronze_ingest  │                    ┌──────────────────────┐
   │ .py (Structured       │───────────────────▶│   SILVER LAYER        │
   │  Streaming job)       │                    │  cleaned, deduped,    │
   └───────────────────────┘                    │  unioned (batch +     │
                                                 │  streaming sensors)  │
                                                 └───────────┬──────────┘
                                                              │
                                                              ▼
                                                 ┌──────────────────────┐
                                                 │   GOLD LAYER          │
                                                 │  business-ready       │
                                                 │  aggregates + LLM     │
                                                 │  sentiment analysis   │
                                                 └───────────┬──────────┘
                                                              │
                                                              ▼
                                                 ┌──────────────────────┐
                                                 │  Databricks AI/BI     │
                                                 │  Dashboard + Genie    │
                                                 │  (natural language Q&A)│
                                                 └──────────────────────┘
```

---

## 3. Tech Stack

| Layer | Technology |
|---|---|
| Cloud platform | Microsoft Azure |
| Data lake | Azure Data Lake Storage Gen2 (ADLS) |
| Real-time messaging | Azure Event Hubs (Kafka-compatible endpoint) |
| Compute & orchestration | Databricks (Serverless compute, Jobs) |
| Table format & governance | Delta Lake + Unity Catalog |
| Streaming engine | Spark Structured Streaming |
| Batch processing | PySpark |
| Secrets management | Databricks Secret Scopes |
| BI / visualization | Databricks AI/BI Dashboards |
| Natural language analytics | Databricks Genie (Genie Code + Genie Spaces) |
| Language | Python 3.10 |

---

## 4. Project Structure

```
databricks_utm_demo_pipeline/
├── config/
│   └── azure_settings.py          # central config: paths, schema names, secrets
├── src/
│   ├── data_generation/
│   │   ├── generate_dimensions.py     # plants, lines, SKUs, customers
│   │   ├── generate_historical.py     # production batches, shipments, audits
│   │   └── generate_streaming.py      # synthetic sensor readings (batch snapshot)
│   ├── azure_sync/
│   │   ├── upload_to_adls.py          # pushes generated files to ADLS Gen2
│   │   └── eventhub_producer.py       # simulates live sensor stream → Event Hubs
│   ├── pipeline/
│   │   ├── bronze_ingest.py           # ADLS files → Bronze tables
│   │   ├── stream_bronze_ingest.py    # Event Hubs → Bronze streaming table
│   │   ├── silver_transform.py        # Bronze → Silver (clean, dedupe, union)
│   │   ├── gold_aggregate.py          # Silver → Gold (business aggregates)
│   │   └── sentiment_analysis.py      # LLM sentiment on quality audit notes
│   └── utils/
│       └── logger.py
├── notebooks/
│   ├── run_pipeline                   # orchestrates Bronze→Silver→Gold→Sentiment
│   └── run_streaming                  # runs stream_bronze_ingest on a schedule
└── requirements.txt
```

---

## 5. Data Model

### Bronze Layer (`utm_demo_catalog.bronze`) — raw, minimally processed

| Table | Source | Description |
|---|---|---|
| `dim_plants` | ADLS file | Plant master data (name, country, capacity) |
| `dim_lines` | ADLS file | Production line master data |
| `dim_sku_catalog` | ADLS file | Product SKU catalog |
| `dim_retail_customers` | ADLS file | Retail customer master data |
| `historical_production_batches` | ADLS file | Batch-level production records |
| `historical_shipments` | ADLS file | Shipment records to customers |
| `historical_quality_audit_logs` | ADLS file | Free-text quality audit notes |
| `sensor_readings` | ADLS file | Synthetic batch snapshot of sensor data |
| `sensor_readings_stream` | **Event Hubs (live)** | Real-time sensor telemetry via Kafka |

### Silver Layer (`utm_demo_catalog.silver`) — cleaned & unified

| Table | Transformation |
|---|---|
| `dim_plants`, `dim_lines`, `dim_sku_catalog`, `dim_retail_customers` | Type casting, deduplication |
| `fact_production_batches` | Casting, yield_pct calculation, dedup by batch_id |
| `fact_shipments` | Casting, dedup by shipment_id |
| `fact_quality_audits` | Dedup by audit_id |
| `fact_sensor_readings` | **Unions `bronze.sensor_readings` (batch) + `bronze.sensor_readings_stream` (live)**, tagged with a `data_source` column (`batch_file` / `event_hub_stream`), deduped by reading_id |

### Gold Layer (`utm_demo_catalog.gold`) — business-ready aggregates

| Table | Purpose |
|---|---|
| `d_production_summary` | Yield %, defect rate %, scrap counts by plant + line |
| `d_retailer_360` | On-time delivery rate, delayed/returned shipment counts by customer |
| `d_quality_audit_summary` | Audit outcome counts by plant |
| `d_audit_sentiment` | LLM-generated sentiment classification of audit notes |
| `d_sensor_health_summary` | Anomaly rate %, avg/max temperature & vibration by line, **split by data_source** so live vs. historical equipment health can be compared directly |

---

## 6. How the Pipeline Runs

### Two independent Databricks Jobs

**Job 1 — `muller_medallion_pipeline`** (the "main" pipeline)
- Runs on a schedule (e.g., hourly/daily)
- Tasks: `bronze_ingest` → `silver_transform` → `gold_aggregate` → `sentiment_analysis`
- Reads static files from ADLS, rebuilds all Silver and Gold tables

**Job 2 — `sensor_streaming_ingest`** (live telemetry ingestion)
- Runs on a tight schedule (every ~5 minutes)
- Uses `trigger(availableNow=True)` — Databricks serverless compute does not support
  always-on `processingTime` triggers, so instead this job wakes up, drains
  whatever new events are sitting in Event Hubs, writes them to
  `bronze.sensor_readings_stream`, and exits
- Depends on the local `eventhub_producer.py` script actively sending events

### Why two jobs instead of one continuous stream

Databricks serverless compute only supports `AvailableNow`/`Once` triggers, not
infinite `processingTime` streaming (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`).
The architecture was adapted to a **frequent micro-batch pattern** instead — this
is also more cost-effective for a project running on Azure student credits than
paying for a permanently-running cluster.

### End-to-end refresh flow

```
Producer (continuous) → Event Hubs → [every 5 min] Streaming Job → Bronze
                                                                       │
                          [on its own schedule] Main Pipeline Job ────┘
                                    │
                                    ▼
                          Silver (union) → Gold
                                    │
                                    ▼
                     Dashboard (auto-refreshes, re-queries Gold)
```

**Important:** the dashboard is only ever as fresh as the *last main pipeline run* —
its own auto-refresh just re-runs SQL against Gold tables, it doesn't trigger new
transformations. For demos, manually clicking "Run now" on the main pipeline job
guarantees the freshest possible numbers on demand.

---

## 7. Dashboard & Genie

Built as a Databricks AI/BI Dashboard on top of the four Gold tables, organized into
management-relevant pages:

1. **Executive Summary** — KPI cards (yield %, defect rate, on-time delivery,
   scrapped batches, sensor anomalies, audit sentiment) with red/amber/green
   status coloring
2. **Production & Quality** — yield and defect rate by plant/line, audit sentiment
   breakdown
3. **Customer & Delivery** — on-time delivery rate by customer, delayed/returned
   shipment counts
4. **Equipment Health Monitoring** — anomaly rate comparison between live
   (`event_hub_stream`) and historical (`batch_file`) sensor data, temperature and
   vibration trends by line

A companion **Genie Space** is enabled on the dashboard, letting non-technical
users type plain-English questions (e.g., *"which plant has the worst defect
rate?"*) and get grounded, SQL-backed answers directly on the page — no
Databricks or SQL knowledge required.

---

## 8. How to Run This Project

### Prerequisites
- Azure subscription with an ADLS Gen2 storage account, Event Hubs namespace, and
  Databricks workspace (Unity Catalog enabled)
- Python 3.10, virtualenv
- Databricks secret scope configured with your Event Hubs connection string:
  ```bash
  databricks secrets create-scope eventhub-secrets
  databricks secrets put-secret eventhub-secrets connection-string
  ```

### Local setup
```bash
git clone <repo-url>
cd databricks_utm_demo_pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export EVENT_HUB_CONNECTION_STR='<your-event-hubs-connection-string>'
export EVENT_HUB_NAME='sensor-telemetry'
```

### Generate & upload synthetic data (one-time)
```bash
python -m src.data_generation.generate_dimensions
python -m src.data_generation.generate_historical
python -m src.data_generation.generate_streaming
python -m src.azure_sync.upload_to_adls
```

### Start the live sensor simulator
```bash
python -m src.azure_sync.eventhub_producer --mode continuous
```

### In Databricks
```python
# Run once to backfill Bronze/Silver/Gold from files
from src.pipeline import bronze_ingest, silver_transform, gold_aggregate
bronze_ingest.run(spark)
silver_transform.run(spark)
gold_aggregate.run(spark)

# Pull in live streaming data
from src.pipeline import stream_bronze_ingest
stream_bronze_ingest.run(spark, dbutils)
```

Or, for production use, deploy `notebooks/run_pipeline` and `notebooks/run_streaming`
as two separate scheduled Databricks Jobs as described in Section 6.

---

## 9. Key Engineering Decisions & Lessons Learned

- **Batch + streaming unification**: rather than treating live sensor data as a
  replacement for historical data, both sources are unioned in Silver with a
  `data_source` tag — preserving full history while still surfacing real-time
  signals.
- **Serverless streaming constraints**: discovered that Databricks serverless
  compute doesn't support infinite `processingTime` triggers, and adapted the
  architecture to a scheduled `AvailableNow` micro-batch pattern instead of an
  always-on cluster — a more cost-effective design for this project's scale.
- **Schema evolution**: Delta tables enforce schema by default; any structural
  change to a table requires `option("overwriteSchema", "true")` on write, which
  became a recurring fix throughout development as the sensor schema evolved.
- **Separation of concerns**: two independent Databricks Jobs (main pipeline vs.
  streaming ingestion) allow each to be scheduled, scaled, and paused
  independently based on actual freshness needs and cost constraints.

---

## 10. Future Enhancements

- Add a monthly/time-series trend table (`d_production_monthly_trend`) to support
  natural-language trend questions like "worst defect trend this month"
  through Genie
- Move `eventhub_producer.py` off a local machine onto a small always-on Azure
  Function or container for a fully cloud-native demo
- Add data quality checks (e.g., Great Expectations or Delta Live Tables
  expectations) between Bronze and Silver
- Add alerting (email/Slack) on the streaming job for anomaly spikes detected in
  `d_sensor_health_summary`
