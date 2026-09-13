import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s' )

logger = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"


def extract(file_path: Path) -> pd.DataFrame:
    if not file_path.is_file():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        raise RuntimeError(f"Failed to read {file_path}: {e}")

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # If column order_date exists, convert it to datetime
    revenue_columns = ['quantity', 'unit_price','discount_amount']

    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'])
    if all(col in df.columns for col in revenue_columns):
        df['gross_revenue'] = df['quantity'] * df['unit_price']
        df['net_revenue'] = df['gross_revenue'] - df['discount_amount']

    return df   


def build_dim_customer(df_customers: pd.DataFrame) -> pd.DataFrame: 

    dim_customer = df_customers[["customer_id", "customer_name", "city", "country"]].copy()

    if dim_customer["customer_id"].duplicated().any():
        raise ValueError("Duplicated customer_id found in dim_customer")

    # Create surrogate key
    dim_customer["customer_key"] = range(1, len(dim_customer) + 1)
    # Reorder columns to place the surrogate key first
    dim_customer = dim_customer[['customer_key'] + [col for col in dim_customer.columns if col != 'customer_key']]

    return dim_customer


def build_dim_product(df_products: pd.DataFrame) -> pd.DataFrame:

    dim_product = df_products[["product_id","product_name","category","brand"]].copy()

    if dim_product["product_id"].duplicated().any():
        raise ValueError("Duplicated product_id found in dim_product")
    
    # Create surrogate key
    dim_product["product_key"] = range(1, len(dim_product) + 1)
    # Reorder columns to place the surrogate key first
    dim_product = dim_product[['product_key'] + [col for col in dim_product.columns if col != 'product_key']]

    return dim_product


def build_dim_store(df_store: pd.DataFrame) -> pd.DataFrame:

    dim_store = df_store[["store_id","store_name","store_city"]].copy()

    if dim_store["store_id"].duplicated().any():
        raise ValueError("Duplicated store_id found in dim_store")

    # Create surrogate key
    dim_store["store_key"] = range(1, len(dim_store) + 1)
    # Reorder columns to place the surrogate key first
    dim_store = dim_store[['store_key'] + [col for col in dim_store.columns if col != 'store_key']]

    return dim_store


def build_dim_date(df_sales: pd.DataFrame) -> pd.DataFrame:

    dim_date = df_sales[['order_date']].copy()
    
    # Clean to get unique dates
    dim_date = dim_date.drop_duplicates(subset=['order_date'])
    dim_date.rename(columns={'order_date':'full_date'}, inplace=True)
    dim_date = dim_date.sort_values("full_date").reset_index(drop=True)
    # Create attributes for the date dimension
    dim_date['day'] = dim_date['full_date'].dt.day
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.month_name()
    dim_date['quarter'] = dim_date['full_date'].dt.quarter
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['day_of_week_num'] = dim_date['full_date'].dt.day_of_week
    dim_date['day_of_week_name'] = dim_date['full_date'].dt.day_name()
    dim_date['is_weekend'] = dim_date['day_of_week_num'] >= 5
    
    # Create key for the date dimension
    dim_date['date_key'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date = dim_date[['date_key'] + [col for col in dim_date.columns if col != 'date_key']]

    return dim_date


def build_fact_sales(dim_customer: pd.DataFrame, dim_product: pd.DataFrame, dim_store: pd.DataFrame, dim_date: pd.DataFrame, df_sales: pd.DataFrame) -> pd.DataFrame:

    fact_sales = df_sales.copy()
    # Original number of rows
    original_row_count = len(fact_sales)
    # Merge with dimension tables to get surrogate keys
    fact_sales = fact_sales.merge(dim_customer[['customer_id', 'customer_key']], on='customer_id', how='left', validate='many_to_one')
    fact_sales = fact_sales.merge(dim_product[['product_id', 'product_key']], on='product_id', how='left', validate='many_to_one')
    fact_sales = fact_sales.merge(dim_store[['store_id', 'store_key']], on='store_id', how='left', validate='many_to_one')
    fact_sales = fact_sales.merge(dim_date[['full_date', 'date_key']], left_on='order_date', right_on='full_date', how='left', validate='many_to_one')
    # Check there are no NULL values in the surrogate key columns
    if fact_sales[['customer_key', 'product_key', 'store_key', 'date_key']].isna().any().any():
        raise ValueError("NULL values found in fact table surrogate keys after merging with dimensions")
    
    if len(fact_sales) != original_row_count:
        raise ValueError("Fact row count changed after dimension merges. Expected row count: {}, Actual row count: {}".format(original_row_count, len(fact_sales)))
    
    # Drop columns that are no longer needed
    fact_sales.drop(columns=['customer_id','product_id','store_id','order_date','full_date'], inplace=True)
    # Select and reorder columns for the fact table + with all columns from fact_sales
    fact_sales = fact_sales[['order_id','date_key', 'customer_key', 'store_key', 'product_key', 'quantity', 'unit_price', 'discount_amount', 'gross_revenue', 'net_revenue']]

    return fact_sales


def validate_model(fact_sales: pd.DataFrame, dim_customer: pd.DataFrame, dim_product: pd.DataFrame, dim_store: pd.DataFrame, dim_date: pd.DataFrame) -> None:
    # Check that all foreign keys in the fact table exist in the corresponding dimension tables
    if not fact_sales['customer_key'].isin(dim_customer['customer_key']).all():
        raise ValueError("Fact table contains customer_key values not present in dim_customer")
    if not fact_sales['product_key'].isin(dim_product['product_key']).all():
        raise ValueError("Fact table contains product_key values not present in dim_product")
    if not fact_sales['store_key'].isin(dim_store['store_key']).all():
        raise ValueError("Fact table contains store_key values not present in dim_store")
    if not fact_sales['date_key'].isin(dim_date['date_key']).all():
        raise ValueError("Fact table contains date_key values not present in dim_date")

    # Check that there are no NULL values in the fact table's foreign keys
    if fact_sales[['customer_key', 'product_key', 'store_key', 'date_key']].isna().any().any():
        raise ValueError("Fact table contains NULL values in foreign key columns")

    # Check that there are no NULL values in the fact table's measure columns
    if fact_sales[['quantity', 'unit_price', 'discount_amount', 'gross_revenue', 'net_revenue']].isna().any().any():
        raise ValueError("Fact table contains NULL values in measure columns")

    # Check that all  measure columns in the fact table are non-negative
    if (fact_sales['quantity'] <= 0).any():
        raise ValueError("quantity must be greater than zero")
    
    if (fact_sales[['unit_price', 'discount_amount', 'gross_revenue', 'net_revenue']] < 0).any().any():
        raise ValueError("Fact table contains negative values in measure columns")

    # Check that gross_revenue is still equal to quantity * unit_price
    if not (fact_sales['gross_revenue'] == fact_sales['quantity'] * fact_sales['unit_price']).all():
        raise ValueError("Fact table contains inconsistent gross_revenue values")
    if not (fact_sales['net_revenue'] == fact_sales['gross_revenue'] - fact_sales['discount_amount']).all():
        raise ValueError("Fact table contains inconsistent net_revenue values")

    # Check that there are no duplicate rows in the dimension tables
    if dim_customer['customer_key'].duplicated().any():
        raise ValueError("Dimension table dim_customer contains duplicate rows")
    if dim_product['product_key'].duplicated().any():
        raise ValueError("Dimension table dim_product contains duplicate rows")
    if dim_store['store_key'].duplicated().any():
        raise ValueError("Dimension table dim_store contains duplicate rows")
    if dim_date['date_key'].duplicated().any():
        raise ValueError("Dimension table dim_date contains duplicate rows")

    # Check that there are no NULL rows in the dimension tables
    if dim_customer['customer_key'].isna().any():
        raise ValueError("Dimension table dim_customer contains NULL values")
    if dim_product['product_key'].isna().any():
        raise ValueError("Dimension table dim_product contains NULL values")
    if dim_store['store_key'].isna().any():
        raise ValueError("Dimension table dim_store contains NULL values")
    if dim_date['date_key'].isna().any():
        raise ValueError("Dimension table dim_date contains NULL values")
    
    # Check also for business key uniqueness in the dimension tables
    if dim_customer['customer_id'].duplicated().any():
        raise ValueError("Dimension table dim_customer contains duplicate business keys")
    if dim_product['product_id'].duplicated().any():
        raise ValueError("Dimension table dim_product contains duplicate business keys")
    if dim_store['store_id'].duplicated().any():
        raise ValueError("Dimension table dim_store contains duplicate business keys")
    if dim_date['full_date'].duplicated().any():
        raise ValueError("Dimension table dim_date contains duplicate business keys")

    # Check also for business key NULL values in the dimension tables
    if dim_customer['customer_id'].isna().any():
        raise ValueError("Dimension table dim_customer contains NULL business keys")
    if dim_product['product_id'].isna().any():
        raise ValueError("Dimension table dim_product contains NULL business keys")
    if dim_store['store_id'].isna().any():
        raise ValueError("Dimension table dim_store contains NULL business keys")
    if dim_date['full_date'].isna().any():
        raise ValueError("Dimension table dim_date contains NULL business keys")

    # Check that there are no duplicate rows in the fact table
    if fact_sales.duplicated(subset=["order_id", "product_key"]).any():
        raise ValueError("Fact table contains duplicate rows")

    if fact_sales['order_id'].isna().any():
        raise ValueError("Fact table contains NULL values in order_id")

    logger.info("Model validation passed successfully")


def load_model(df: pd.DataFrame, output_path: Path):
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
 
    logger.info("Model loaded successfully")



def main() -> None:

    try:
        logger.info('Extracting data from raw CSV files')
        df_customers = extract(RAW_DIR / "customers.csv")
        logger.info("Shape: %s", df_customers.shape)
        logger.info("Customers preview:\n%s", df_customers.head())
        df_products = extract(RAW_DIR / "products.csv")
        logger.info("Shape: %s", df_products.shape)
        logger.info("Products preview:\n%s", df_products.head())
        df_sales = extract(RAW_DIR / "sales.csv")
        logger.info("Shape: %s", df_sales.shape)
        logger.info("Sales preview:\n%s", df_sales.head())
        df_stores = extract(RAW_DIR / "stores.csv")
        logger.info("Shape: %s", df_stores.shape)
        logger.info("Stores preview:\n%s", df_stores.head())
    except Exception as e:
        logger.exception("Failed to extract data: %s", e)
        raise

    try:
        logger.info('Transforming extracted data')
        df_customers = transform(df_customers)
        df_products = transform(df_products)
        df_sales = transform(df_sales)
        df_stores = transform(df_stores)
        logger.info('Transformed all data successfully')
    except Exception as e:
        logger.exception("Failed to transform data: %s", e)
        raise
  
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Building dimensional models")
        dim_customer = build_dim_customer(df_customers)
        dim_product = build_dim_product(df_products)
        dim_store = build_dim_store(df_stores)
        dim_date = build_dim_date(df_sales)
        logger.info("Dimensional models built successfully")
    except Exception as e:
        logger.exception("Failed to build dimensional models: %s", e)
        raise

    try:
        logger.info("Building fact_sales table")
        fact_sales = build_fact_sales(dim_customer, dim_product, dim_store, dim_date, df_sales)
        logger.info("Fact_sales table built successfully")
    except Exception as e:
        logger.exception("Failed to build fact_sales table: %s", e)
        raise

    try:
        logger.info("Validating model")
        validate_model(fact_sales, dim_customer, dim_product, dim_store, dim_date)
        logger.info("Model validation passed successfully")
    except Exception as e:
        logger.exception("Failed to validate model: %s", e)
        raise

    try:
        logger.info("Saving processed data to CSV files")
        load_model(dim_customer, PROCESSED_DIR / "dim_customer.csv")
        load_model(dim_product, PROCESSED_DIR / "dim_product.csv")
        load_model(dim_store, PROCESSED_DIR / "dim_store.csv")
        load_model(dim_date, PROCESSED_DIR / "dim_date.csv")
        load_model(fact_sales, PROCESSED_DIR / "fact_sales.csv")
        logger.info("Processed data saved successfully")
    except Exception as e:
        logger.exception("Failed to save processed data: %s", e)
        raise

if __name__ == "__main__":
    main()


