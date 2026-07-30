import sys
import os

_current = os.path.dirname(os.path.abspath(__file__))
for _ in range(6):
    if os.path.isdir(os.path.join(_current, "config")):
        if _current not in sys.path:
            sys.path.insert(0, _current)
        break
    _current = os.path.dirname(_current)

"""
Continuously generates sensor readings and streams them to Azure Event Hubs.
Requires: pip install azure-eventhub
Auth: set EVENT_HUB_CONNECTION_STR and EVENT_HUB_NAME as environment variables.
"""
import os
import sys
import time
import json
import numpy as np
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import settings, azure_settings
from src.utils.logger import get_logger
from src.data_generation.generate_streaming import generate_sensor_events

logger = get_logger(__name__)


def get_producer():
    from azure.eventhub import EventHubProducerClient

    if not azure_settings.EVENT_HUB_CONNECTION_STR:
        raise ValueError("EVENT_HUB_CONNECTION_STR environment variable is not set.")

    return EventHubProducerClient.from_connection_string(
        conn_str=azure_settings.EVENT_HUB_CONNECTION_STR,
        eventhub_name=azure_settings.EVENT_HUB_NAME
    )


def run_continuous(batch_size_range=(5, 11), interval_seconds=None):
    from azure.eventhub import EventData

    interval_seconds = interval_seconds or settings.STREAM_EVENT_INTERVAL_SECONDS
    line_ids = [f"L{i:03d}" for i in range(1, 33)]

    producer = get_producer()
    logger.info(f"Connected to Event Hub '{azure_settings.EVENT_HUB_NAME}'. Starting continuous stream...")

    try:
        with producer:
            while True:
                batch = producer.create_batch()
                n_events = np.random.randint(*batch_size_range)
                events = generate_sensor_events(line_ids, n_events=n_events, start_time=datetime.utcnow())
                for e in events:
                    batch.add(EventData(json.dumps(e)))
                producer.send_batch(batch)
                logger.info(f"Sent {len(batch)} events to Event Hubs.")
                time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user (Ctrl+C).")


def send_single_batch(n_events=None):
    """One-shot send, useful for testing or scheduled (non-continuous) triggers."""
    from azure.eventhub import EventData

    line_ids_path = os.path.join(settings.RAW_DIM_DIR, "dim_lines.csv")
    if os.path.exists(line_ids_path):
        import pandas as pd
        line_ids = pd.read_csv(line_ids_path)["line_id"].values
    else:
        line_ids = [f"L{i:03d}" for i in range(1, 33)]

    events = generate_sensor_events(line_ids, n_events=n_events)
    producer = get_producer()
    with producer:
        batch = producer.create_batch()
        for e in events:
            batch.add(EventData(json.dumps(e)))
        producer.send_batch(batch)
    logger.info(f"Sent one-shot batch of {len(events)} events to Event Hubs.")
    return len(events)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["continuous", "once"], default="once")
    args = parser.parse_args()

    if args.mode == "continuous":
        run_continuous()
    else:
        send_single_batch()