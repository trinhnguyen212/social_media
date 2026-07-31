import logging
import os
import sys
import time
from dotenv import load_dotenv

# Import ETL components
from pipeline.ingestion import run as run_ingestion
from pipeline.transform import transform_raw_data
from pipeline.load import load_data
from warehouse.postgres import PostgresStorage

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("etl_orchestrator")

def main():
    """
    Main entry point for the Social Media ETL Pipeline.

    Workflow:
      1. Setup: Initialize DB tables
      2. Extract: Run Facebook ingestion (Raw JSON landing)
      3. Transform: Clean and flatten JSON to DataFrames
      4. Load: Upsert DataFrames into PostgreSQL Warehouse
    """
    start_time = time.perf_counter()
    logger.info("🚀 Starting Social Media ETL Pipeline...")

    try:
        # --- Step 0: Setup ---
        logger.info("Initializing Warehouse...")
        storage = PostgresStorage()
        storage.create_db_and_tables()

        # --- Step 1: Extract (Ingestion) ---
        logger.info("Phase 1/3: Ingestion (Extract)")
        run_ingestion()

        # --- Step 2: Transform ---
        logger.info("Phase 2/3: Transformation (Transform)")
        raw_data_dir = os.environ.get("OUTPUT_DIR", "data/raw")
        df_posts, df_comments = transform_raw_data(raw_data_dir)

        logger.info(f"Transformation produced: {len(df_posts)} posts, {len(df_comments)} comments")

        # --- Step 3: Load ---
        logger.info("Phase 3/3: Loading (Load)")
        posts_synced, comments_synced = load_data(storage, df_posts, df_comments)

        # --- Final Summary ---
        elapsed = time.perf_counter() - start_time
        logger.info("="*50)
        logger.info("✅ ETL Pipeline Run Completed Successfully")
        logger.info(f"Total Duration: {elapsed:.2f}s")
        logger.info(f"Warehouse Sync: {posts_synced} posts, {comments_synced} comments")
        logger.info("="*50)

    except Exception as e:
        logger.error(f"❌ Pipeline failed with a critical error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
