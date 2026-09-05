import pandas as pd
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
FILE_PATH = PROJECT_DIR / 'data' / 'raw' / 'sales.csv'
OUTPUT_PATH = PROJECT_DIR / 'data' / 'processed' / 'sales_processed.csv'

def extract(file_path: Path) -> pd.DataFrame:

    df = pd.read_csv(file_path)

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()
    df['total_amount'] = df['quantity'] * df['unit_price']
    return df


def load(df: pd.DataFrame, output_path: Path) -> None:
    df.to_csv(output_path, index=False)


def main() -> None:
    raw_df = extract(FILE_PATH)
    transformed_df = transform(raw_df)
    load(transformed_df, OUTPUT_PATH)

if __name__ == '__main__':
    main()


