from pathlib import Path
import psycopg
import logging
from datetime import datetime
from uuid import uuid4
from src.database import DB_CONFIG
from src.extract import extract_csv
from src.transform import validate_orders
from src.load import load_staging, apply_upsert
from src.metadata import start_run, finish_run_success, finish_run_failure


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = PROJECT_ROOT / "data" / "end_to_end_good.csv"



def main():

    run_id = str(uuid4())
    started_at = datetime.now()
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        start_run(conn, run_id, started_at)

    try:
        logger.info("Starting ETL process")
        # Extract data
        df = extract_csv(CSV_PATH)

        # Transform data
        df = validate_orders(df)

        # Load data into staging
        with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
            with conn.transaction():
                staged_rows = load_staging(conn, df)
                affected_rows = apply_upsert(conn)

    except Exception as e:
        finished_at = datetime.now()
        with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
            finish_run_failure(conn, run_id, finished_at, str(e))
        logger.exception("ETL process failed")
        raise
    finished_at = datetime.now()
    try:
        with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
            finish_run_success(conn, run_id, finished_at, staged_rows, affected_rows)
    except Exception:
        logger.exception("ETL committed successfully, but metadata update failed")
        raise




    logger.info("ETL process completed")

if __name__ == "__main__":
    main()