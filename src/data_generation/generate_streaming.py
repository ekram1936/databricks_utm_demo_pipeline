"""
Generates streaming sensor readings simulating the Azure Event Hubs source.
Each run writes a fresh timestamped batch to data/raw/streaming/.
"""
import numpy as np
import pandas as pd
import json
import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def generate_sensor_events(line_ids, n_events=None, start_time=None) -> list:
    logger.info("Generating streaming sensor readings...")
    rng = np.random.default_rng()
    n_events = n_events or settings.VOLUMES["n_sensor_stream_events"]
    start_time = start_time or datetime.now()

    active_lines = rng.choice(line_ids, n_events)
    events = []
    for i in range(n_events):
        ts = start_time + timedelta(seconds=int(i * settings.STREAM_EVENT_INTERVAL_SECONDS))
        line = active_lines[i]
        is_anomaly = rng.random() < 0.03

        base_temp = 72 + rng.normal(0, 0.5)
        base_pressure = 3.2 + rng.normal(0, 0.08)
        vib = abs(rng.normal(1.2, 0.3))

        if is_anomaly:
            base_temp += rng.choice([-1, 1]) * rng.uniform(4, 9)
            base_pressure += rng.choice([-1, 1]) * rng.uniform(0.5, 1.2)
            vib += rng.uniform(2, 5)

        event = {
            "reading_id": f"SR{rng.integers(100000,999999)}_{i}",
            "line_id": str(line),
            "event_timestamp": ts.isoformat(),
            "temperature_c": round(float(base_temp), 2),
            "pressure_bar": round(float(base_pressure), 3),
            "fill_volume_ml": round(float(rng.normal(500, 2.5)), 2),
            "vibration_mm_s": round(float(vib), 3),
            "machine_status": str(rng.choice(["Running", "Idle", "Down-Changeover", "Down-Fault"], p=[0.82, 0.08, 0.06, 0.04])),
            "anomaly_flag": int(is_anomaly)
        }
        events.append(event)

    logger.info(f"Generated {len(events)} streaming sensor events.")
    return events


def run():
    os.makedirs(settings.RAW_STREAMING_DIR, exist_ok=True)

    lines_path = os.path.join(settings.RAW_DIM_DIR, "dim_lines.csv")
    lines = pd.read_csv(lines_path)

    events = generate_sensor_events(lines["line_id"].values)

    out_file = os.path.join(
        settings.RAW_STREAMING_DIR,
        f"sensor_readings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    )
    with open(out_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    logger.info(f"Streaming batch written to {out_file}")
    return events


if __name__ == "__main__":
    run()
