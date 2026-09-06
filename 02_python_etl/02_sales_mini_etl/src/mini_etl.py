import pandas as pd
from pathlib import Path
import logging

PROJECT_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_DIR / "data" / "raw"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

INPUT_PATH = RAW_DIR / 'raw_sales.csv'
CLEAN_OUTPUT_PATH = PROCESSED_DIR / 'clean_sales.csv'
SUMMARY_OUTPUT_PATH = PROCESSED_DIR / 'sales_summary.csv'

REQUIRED_COLUMNS = {'order_id', 'customer_id', 'order_date', 'product', 'category', 'quantity', 'unit_price', 'country'}
TEXT_COLUMNS = {'customer_id','product', 'category', 'country'}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s' )

logger = logging.getLogger(__name__)


def extract(file_path: Path) -> pd.DataFrame:
    if not file_path.is_file():
        raise FileNotFoundError(f'Input file not found: {file_path}')

    try:
        df = pd.read_csv(file_path)
    except pd.errors.EmptyDataError as e:
        raise ValueError('File contains no readable data') from e

    if df.empty:
        raise ValueError('File is empty')

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    rows_before = len(df)
    # Ensure all required columns are present
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(f'Missing required columns: {missing_columns}')

    # Drop exact duplicates
    df = df.drop_duplicates()
    rows_removed = rows_before - len(df)
    logger.info(f'Removed {rows_removed} duplicate rows')

    # Clean string whitespace in text columns
    for column in TEXT_COLUMNS:
        df[column] = df[column].astype("string").str.strip()

    rows_before = len(df)
    df = df.dropna(subset=['country'])
    rows_removed = rows_before - len(df)
    logger.info(f'Removed {rows_removed} rows with missing country')

    # Normalize country and category
    df['country'] = df['country'].str.capitalize()
    df['category'] = df['category'].str.capitalize()

    # Ensure unit_price has correct separator
    df['unit_price'] = df['unit_price'].astype("string").str.replace(",", ".", regex=False )

    # Clean numerical columns
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    
    # Drop rows with any NaN values in quantity or unit_price
    rows_before = len(df)
    df = df.dropna(subset=['quantity', 'unit_price'])
    rows_removed = rows_before - len(df)
    logger.info(f'Removed {rows_removed} rows with invalid quantity or unit_price')
    
    # Eliminate non-possible values 
    rows_before = len(df)
    df = df[(df['quantity'] > 0)]
    rows_removed = rows_before - len(df)
    logger.info(f'Removed {rows_removed} rows with non-positive quantity')
    rows_before = len(df)
    df = df[(df['unit_price'] > 0)]
    rows_removed = rows_before - len(df)
    logger.info(f'Removed {rows_removed} rows with non-positive unit_price')

    df['quantity'] = df['quantity'].astype('int64')
  
    # Take care of order_date column
    df['order_date'] = pd.to_datetime(df['order_date'], 
            format='mixed',
            dayfirst=True,
            errors='coerce')
    
    rows_before = len(df)
    df = df.dropna(subset=['order_date'])
    rows_removed = rows_before - len(df)
    logger.info(f'Removed {rows_removed} rows with invalid order_date')

    # Create total_amount column
    df['total_amount'] = df['quantity'] * df['unit_price']
  
    return df


def create_summary(df: pd.DataFrame) -> pd.DataFrame:

    df_summary = df.groupby(['country', 'category']).agg(
        number_of_orders=('order_id', 'nunique'),
        units_sold=('quantity', 'sum'),
        total_revenue=('total_amount', 'sum')
    ).reset_index()

    #Sort the summary by total_revenue
    df_summary = df_summary.sort_values(by='total_revenue', ascending=False)

    return df_summary


def load(df: pd.DataFrame, output_path: Path) -> None:

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def main() -> None:

    logger.info('Starting ETL process')

    try:

        df = extract(INPUT_PATH)
        logger.info('Extracted %s raw rows', len(df))
        clean_df = transform(df)
        logger.info('Transformed raw data into %s clean rows', len(clean_df))
        summary_df = create_summary(clean_df)
        logger.info('Created summary data with %s groups', len(summary_df))
        load(clean_df, CLEAN_OUTPUT_PATH)
        logger.info(f'Loaded clean data to {CLEAN_OUTPUT_PATH}')
        load(summary_df, SUMMARY_OUTPUT_PATH)
        logger.info(f'Loaded summary data to {SUMMARY_OUTPUT_PATH}')

    except Exception:
        logger.exception('ETL process failed')
        raise

    logger.info('ETL process completed successfully')
    


if __name__ == "__main__":
    main()