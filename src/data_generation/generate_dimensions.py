"""
Generates all dimension tables: plants, production lines, SKU catalog, retail customers.
"""
import numpy as np
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def generate_dim_plants() -> pd.DataFrame:
    logger.info("Generating dim_plants...")
    df = pd.DataFrame(settings.PLANTS)
    logger.info(f"dim_plants generated with {len(df)} rows.")
    return df


def generate_dim_lines() -> pd.DataFrame:
    logger.info("Generating dim_lines...")
    rng = np.random.default_rng(settings.RANDOM_SEED)
    rows = []
    lid = 1
    plant_ids = [p["plant_id"] for p in settings.PLANTS]
    for p in plant_ids:
        for lt in settings.LINE_TYPES:
            for n in range(1, settings.VOLUMES["n_lines_per_type_per_plant"] + 1):
                rows.append({
                    "line_id": f"L{lid:03d}",
                    "plant_id": p,
                    "line_type": lt,
                    "line_name": f"{lt}-{n}",
                    "install_year": int(rng.integers(2005, 2023))
                })
                lid += 1
    df = pd.DataFrame(rows)
    logger.info(f"dim_lines generated with {len(df)} rows.")
    return df


def generate_dim_sku_catalog() -> pd.DataFrame:
    logger.info("Generating dim_sku_catalog...")
    rng = np.random.default_rng(settings.RANDOM_SEED)
    n = settings.VOLUMES["n_skus"]
    df = pd.DataFrame({
        "sku_id": [f"SKU{500+i}" for i in range(n)],
        "sku_name": settings.SKU_NAMES[:n],
        "product_family": rng.choice(["Fresh Dairy", "Yogurt", "Cheese", "Beverage"], n),
        "unit_weight_g": rng.choice([150, 200, 250, 400, 500, 1000, 1500], n),
        "shelf_life_days": rng.choice([10, 14, 21, 30, 45], n)
    })
    logger.info(f"dim_sku_catalog generated with {len(df)} rows.")
    return df


def generate_dim_retail_customers() -> pd.DataFrame:
    logger.info("Generating dim_retail_customers...")
    rng = np.random.default_rng(settings.RANDOM_SEED)
    n = settings.VOLUMES["n_retail_customers"]
    df = pd.DataFrame({
        "customer_id": [f"RC{100+i}" for i in range(n)],
        "customer_name": rng.choice(settings.RETAILERS, n),
        "country": rng.choice(["Germany", "Poland", "Czech Republic", "UK", "Italy"], n, p=[0.6, 0.15, 0.1, 0.1, 0.05]),
        "account_tier": rng.choice(["Key Account", "Regional", "Local"], n, p=[0.3, 0.4, 0.3]),
        "contract_start": pd.to_datetime("2018-01-01") + pd.to_timedelta(rng.integers(0, 2500, n), unit="D")
    })
    logger.info(f"dim_retail_customers generated with {len(df)} rows.")
    return df


def run():
    os.makedirs(settings.RAW_DIM_DIR, exist_ok=True)

    plants = generate_dim_plants()
    lines = generate_dim_lines()
    skus = generate_dim_sku_catalog()
    customers = generate_dim_retail_customers()

    plants.to_csv(os.path.join(settings.RAW_DIM_DIR, "dim_plants.csv"), index=False)
    lines.to_csv(os.path.join(settings.RAW_DIM_DIR, "dim_lines.csv"), index=False)
    skus.to_csv(os.path.join(settings.RAW_DIM_DIR, "dim_sku_catalog.csv"), index=False)
    customers.to_csv(os.path.join(settings.RAW_DIM_DIR, "dim_retail_customers.csv"), index=False)

    logger.info("All dimension tables written to raw/dim/.")
    return plants, lines, skus, customers


if __name__ == "__main__":
    run()
