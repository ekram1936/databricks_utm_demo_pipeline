# Muller AI Pipeline - Full Modular Project

Production-style Databricks + Azure Medallion pipeline simulating Theo Muller Group's
dairy manufacturing data platform.

## Folder Structure

```
muller_ai_pipeline/
├── config/
│   ├── settings.py           # Local data-gen volumes and master data
│   └── azure_settings.py     # Azure/Databricks connection config (env-var based)
├── data/raw/                 # Locally generated CSV/JSONL (source of truth for upload)
├── src/
│   ├── data_generation/      # generate_dimensions, generate_historical, generate_streaming
│   ├── azure_sync/
│   │   ├── upload_to_adls.py       # syncs data/raw/ -> ADLS Gen2
│   │   └── eventhub_producer.py    # sends sensor events to Azure Event Hubs (continuous or one-shot)
│   ├── pipeline/
│   │   ├── bronze_ingest.py        # ADLS -> Unity Catalog bronze
│   │   ├── silver_transform.py     # bronze -> silver (clean/dedupe)
│   │   ├── gold_aggregate.py       # silver -> gold (OEE, retailer 360, audit summary)
│   │   └── sentiment_analysis.py   # LLM sentiment on audit_notes via ai_query()
│   └── utils/logger.py
├── notebooks/run_pipeline.py       # Databricks notebook orchestrator (Bronze->Silver->Gold->LLM)
├── databricks_job.yml              # Databricks Jobs schedule definition
├── main.py                         # local synthetic data generation entry point
└── requirements.txt
```

## How to Run End-to-End

### 1. Generate synthetic data locally
```bash
pip install -r requirements.txt
python main.py
```

### 2. Sync to Azure (ADLS Gen2)
Set environment variables first:
```bash
export AZURE_STORAGE_ACCOUNT=utmdemo26
export AZURE_RAW_CONTAINER=utmdemo-raw
export AZURE_STORAGE_CONNECTION_STRING="<your-connection-string>"   # or use az login for DefaultAzureCredential
python -m src.azure_sync.upload_to_adls
```

### 3. Stream live sensor data to Event Hubs
```bash
export EVENT_HUB_CONNECTION_STR="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
export EVENT_HUB_NAME=sensor-telemetry
python -m src.azure_sync.eventhub_producer --mode continuous
```

### 4. Run the Databricks pipeline
Upload this whole repo to a Databricks Repo, open `notebooks/run_pipeline.py`, attach to a
cluster/serverless compute, and click "Run All". This executes, in order:
1. `bronze_ingest.run(spark)` — raw files -> `utm_demo_catalog.bronze.*`
2. `silver_transform.run(spark)` — cleaned/deduped -> `utm_demo_catalog.silver.*`
3. `gold_aggregate.run(spark)` — business aggregates -> `utm_demo_catalog.gold.*`
4. `sentiment_analysis.run(spark)` — LLM sentiment -> `utm_demo_catalog.gold.d_audit_sentiment`

### 5. (Optional) Schedule it
Use `databricks_job.yml` with Databricks Asset Bundles, or import the same logic into
the Jobs UI, to run the pipeline on a schedule (e.g., hourly) instead of manually.

## Environment Variables Reference

| Variable | Used by | Purpose |
|---|---|---|
| AZURE_STORAGE_ACCOUNT | azure_settings, upload_to_adls | ADLS Gen2 account name |
| AZURE_RAW_CONTAINER | azure_settings, upload_to_adls | Container name (e.g. utmdemo-raw) |
| AZURE_STORAGE_CONNECTION_STRING | upload_to_adls | Auth (alt: az login) |
| EVENT_HUB_CONNECTION_STR | eventhub_producer | Event Hubs namespace auth |
| EVENT_HUB_NAME | eventhub_producer | Event Hub instance name |
| DATABRICKS_CATALOG | azure_settings, pipeline/* | Unity Catalog name (e.g. utm_demo_catalog) |
