"""
Central configuration for synthetic data generation volumes, paths, and seed.
"""
import os

RANDOM_SEED = 42

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")

RAW_DIM_DIR = os.path.join(DATA_ROOT, "raw", "dim")
RAW_HISTORICAL_DIR = os.path.join(DATA_ROOT, "raw", "historical")
RAW_STREAMING_DIR = os.path.join(DATA_ROOT, "raw", "streaming")
BRONZE_DIR = os.path.join(DATA_ROOT, "bronze")
SILVER_DIR = os.path.join(DATA_ROOT, "silver")
GOLD_DIR = os.path.join(DATA_ROOT, "gold")

VOLUMES = {
    "n_plants": 4,
    "n_lines_per_type_per_plant": 2,
    "n_skus": 15,
    "n_retail_customers": 25,
    "n_production_batches": 3000,
    "n_shipments": 5000,
    "n_quality_audits": 1500,
    "n_sensor_stream_events": 800,
}

PLANTS = [
    {"plant_id": "P01", "plant_name": "Freising", "country": "Germany", "capacity_liters_day": 450000, "opened_year": 1972},
    {"plant_id": "P02", "plant_name": "Leppersdorf", "country": "Germany", "capacity_liters_day": 600000, "opened_year": 1990},
    {"plant_id": "P03", "plant_name": "Aretsried", "country": "Germany", "capacity_liters_day": 380000, "opened_year": 1985},
    {"plant_id": "P04", "plant_name": "Droitwich Spa", "country": "UK", "capacity_liters_day": 300000, "opened_year": 2005},
]

LINE_TYPES = ["Pasteurizer", "Homogenizer", "Filling-Line", "Packaging-Line"]

SKU_NAMES = [
    "Yogurt Natural 500g", "Yogurt Strawberry 150g", "Fresh Milk 1.5L", "Quark 500g", "Butter 250g",
    "Cream Cheese 200g", "Skyr 400g", "Buttermilk 1L", "Whipping Cream 200ml", "Kefir 500ml",
    "Mozzarella 250g", "Fresh Milk 1L", "Yogurt Vanilla 150g", "Sour Cream 200g", "Milk Drink 250ml"
]

RETAILERS = ["Rewe", "Edeka", "Aldi Sud", "Aldi Nord", "Lidl", "Kaufland", "dm", "Netto", "Real", "Globus"]

HIST_START_DATE = "2023-01-01"
HIST_DAYS_RANGE = 900

STREAM_EVENT_INTERVAL_SECONDS = 7
