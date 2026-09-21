from pathlib import Path

import pandas as pd
import psycopg

from database import DB_CONFIG


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PROJECT_ROOT / "data" / "incremental_edge_cases.csv"


df = pd.read_csv(CSV_PATH)

rows = list(df.itertuples(index=False, name=None))

print(f"Rows in incremental batch: {len(rows)}")


with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:

    with conn.cursor() as cur:

        with conn.transaction():

            cur.execute("TRUNCATE TABLE staging.orders;")

            cur.executemany(
                """
                INSERT INTO staging.orders (
                    order_id,
                    line_id,
                    order_date,
                    customer_id,
                    product_id,
                    quantity,
                    unit_price,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                rows,
            )


print("Incremental batch loaded into staging.")