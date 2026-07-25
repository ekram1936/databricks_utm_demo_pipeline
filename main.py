"""
Main entry point to generate all synthetic data for the Muller AI Pipeline project.

Usage:
    python main.py

This will:
1. Generate dimension tables (plants, lines, sku catalog, retail customers)
2. Generate historical fact tables (production batches, shipments, quality audits)
3. Generate one batch of streaming sensor readings (simulating Event Hubs)

All outputs are written under data/raw/ and all steps are logged to logs/.
"""
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.logger import get_logger
from src.data_generation import generate_dimensions, generate_historical, generate_streaming

logger = get_logger("main")


def main():
    start = time.time()
    logger.info("=" * 60)
    logger.info("STARTING MULLER AI PIPELINE - SYNTHETIC DATA GENERATION")
    logger.info("=" * 60)

    try:
        logger.info("STEP 1/3: Generating dimension tables...")
        generate_dimensions.run()
        logger.info("STEP 1/3 COMPLETE.")

        logger.info("STEP 2/3: Generating historical fact tables...")
        generate_historical.run()
        logger.info("STEP 2/3 COMPLETE.")

        logger.info("STEP 3/3: Generating streaming sensor readings...")
        generate_streaming.run()
        logger.info("STEP 3/3 COMPLETE.")

        elapsed = round(time.time() - start, 2)
        logger.info(f"ALL DATA GENERATION COMPLETE in {elapsed}s.")
        logger.info("Raw data available under data/raw/{dim,historical,streaming}/")

    except Exception as e:
        logger.exception(f"Pipeline failed with error: {e}")
        raise


if __name__ == "__main__":
    main()
