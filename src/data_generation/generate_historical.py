"""
Generates historical fact tables: production batches, shipments, quality audit logs.
Simulates the one-time / batch load source from Azure SQL Database.
"""
import numpy as np
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def generate_production_batches(plant_ids, line_ids, sku_ids) -> pd.DataFrame:
    logger.info("Generating historical_production_batches...")
    rng = np.random.default_rng(settings.RANDOM_SEED)
    n = settings.VOLUMES["n_production_batches"]
    batch_start = datetime.strptime(settings.HIST_START_DATE, "%Y-%m-%d")

    df = pd.DataFrame({
        "batch_id": [f"B{100000+i}" for i in range(n)],
        "plant_id": rng.choice(plant_ids, n),
        "line_id": rng.choice(line_ids, n),
        "sku_id": rng.choice(sku_ids, n),
        "production_start": [batch_start + timedelta(minutes=int(x)) for x in rng.integers(0, settings.HIST_DAYS_RANGE * 24 * 60, n)],
        "planned_qty_units": rng.integers(2000, 20000, n),
    })
    df["actual_qty_units"] = (df["planned_qty_units"] * rng.uniform(0.92, 1.02, n)).astype(int)
    df["defect_units"] = (df["actual_qty_units"] * rng.beta(1.5, 40, n)).astype(int)
    df["batch_status"] = rng.choice(["Completed", "Completed", "Completed", "Reworked", "Scrapped"], n)

    logger.info(f"historical_production_batches generated with {len(df)} rows.")
    return df


def generate_shipments(batch_ids, customer_ids) -> pd.DataFrame:
    logger.info("Generating historical_shipments...")
    rng = np.random.default_rng(settings.RANDOM_SEED + 1)
    n = settings.VOLUMES["n_shipments"]
    batch_start = datetime.strptime(settings.HIST_START_DATE, "%Y-%m-%d")

    df = pd.DataFrame({
        "shipment_id": [f"SH{200000+i}" for i in range(n)],
        "batch_id": rng.choice(batch_ids, n),
        "customer_id": rng.choice(customer_ids, n),
        "ship_date": [batch_start + timedelta(minutes=int(x)) for x in rng.integers(0, settings.HIST_DAYS_RANGE * 24 * 60, n)],
        "qty_units": rng.integers(500, 8000, n),
        "shipment_status": rng.choice(["Delivered", "In Transit", "Delayed", "Returned"], n, p=[0.85, 0.08, 0.05, 0.02])
    })
    logger.info(f"historical_shipments generated with {len(df)} rows.")
    return df


def generate_quality_audit_logs(batch_ids) -> pd.DataFrame:
    logger.info("Generating historical_quality_audit_logs...")
    rng = np.random.default_rng(settings.RANDOM_SEED + 2)
    n = settings.VOLUMES["n_quality_audits"]

    qa_texts_pass = ["Batch within spec, no deviations.", "QA passed, standard fat content.", "All parameters nominal."]
    qa_texts_fail = ["Fill volume below tolerance.", "Microbial count exceeded limit.", "Packaging seal defect detected."]
    qa_texts_watch = ["Slight temperature deviation, monitored.", "Borderline pH, passed with note.", "Minor visual defect, within limit."]

    outcomes = rng.choice(["Pass", "Fail", "Watch"], n, p=[0.82, 0.06, 0.12])
    notes = [
        rng.choice(qa_texts_pass) if o == "Pass" else rng.choice(qa_texts_fail) if o == "Fail" else rng.choice(qa_texts_watch)
        for o in outcomes
    ]

    df = pd.DataFrame({
        "audit_id": [f"QA{3000+i}" for i in range(n)],
        "batch_id": rng.choice(batch_ids, n),
        "audit_date": pd.to_datetime("2023-06-01") + pd.to_timedelta(rng.integers(0, 700, n), unit="D"),
        "audit_outcome": outcomes,
        "audit_notes": notes
    })
    logger.info(f"historical_quality_audit_logs generated with {len(df)} rows.")
    return df


def run():
    os.makedirs(settings.RAW_HISTORICAL_DIR, exist_ok=True)
    os.makedirs(settings.RAW_DIM_DIR, exist_ok=True)

    plants = pd.read_csv(os.path.join(settings.RAW_DIM_DIR, "dim_plants.csv"))
    lines = pd.read_csv(os.path.join(settings.RAW_DIM_DIR, "dim_lines.csv"))
    skus = pd.read_csv(os.path.join(settings.RAW_DIM_DIR, "dim_sku_catalog.csv"))
    customers = pd.read_csv(os.path.join(settings.RAW_DIM_DIR, "dim_retail_customers.csv"))

    batches = generate_production_batches(plants["plant_id"].values, lines["line_id"].values, skus["sku_id"].values)
    shipments = generate_shipments(batches["batch_id"].values, customers["customer_id"].values)
    audits = generate_quality_audit_logs(batches["batch_id"].values)

    batches.to_csv(os.path.join(settings.RAW_HISTORICAL_DIR, "historical_production_batches.csv"), index=False)
    shipments.to_csv(os.path.join(settings.RAW_HISTORICAL_DIR, "historical_shipments.csv"), index=False)
    audits.to_csv(os.path.join(settings.RAW_HISTORICAL_DIR, "historical_quality_audit_logs.csv"), index=False)

    logger.info("All historical fact tables written to raw/historical/.")
    return batches, shipments, audits


if __name__ == "__main__":
    run()
