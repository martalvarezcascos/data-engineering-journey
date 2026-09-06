import pandas as pd
from pathlib import Path
import logging

PROJECT_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_DIR / 'data' / 'raw'
PROCESSED_DIR = PROJECT_DIR / 'data' / 'processed'

INPUT_FILENAME = 'sales.csv'
OUTPUT_FILENAME = 'sales_processed.csv'

INPUT_PATH = RAW_DIR / INPUT_FILENAME
OUTPUT_PATH = PROCESSED_DIR / OUTPUT_FILENAME

REQUIRED_COLUMNS = {'quantity', 'unit_price'}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
                    )

logger = logging.getLogger(__name__)


def extract(file_path: Path) -> pd.DataFrame:
    if not file_path.is_file():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except pd.errors.EmptyDataError as e:
        raise ValueError("Input file contains no readable data") from e

    if df.empty:
        raise ValueError("Input file is empty")

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:

    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    df = df.copy()
    df['total_amount'] = df['quantity'] * df['unit_price']

    return df


def load(df: pd.DataFrame, output_path: Path) -> None:

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)


def main() -> None:

    logger.info('Starting ETL pipeline')

    try:
        raw_df = extract(INPUT_PATH)
        logger.info('Extracted %s rows', len(raw_df))

        transformed_df = transform(raw_df)
        logger.info('Transformation completed')

        load(transformed_df, OUTPUT_PATH)
        logger.info('Loaded %s rows to %s', len(transformed_df), OUTPUT_PATH)

    except Exception:
        logger.exception('ETL pipeline failed')
        raise

    logger.info('ETL pipeline completed successfully')

if __name__ == '__main__':
    main()


