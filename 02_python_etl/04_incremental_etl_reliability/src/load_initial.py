from pathlib import Path
import pandas as pd
import psycopg
from database import DB_CONFIG


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CSV_PATH = PROJECT_ROOT / "data" / "initial_orders.csv"


df = pd.read_csv(CSV_PATH)

print(f"Rows read from CSV: {len(df)}")


with psycopg.connect(**DB_CONFIG) as conn:

    with conn.cursor() as cur:

        for _, row in df.iterrows():

            cur.execute(
                """
                INSERT INTO staging.orders (
                    order_id,
                    line_id,
                    order_date,
                    customer_id,
                    product_id,
                    quantity,
                    unit_price
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    row["order_id"],
                    row["line_id"],
                    row["order_date"],
                    row["customer_id"],
                    row["product_id"],
                    row["quantity"],
                    row["unit_price"],
                ),
            )

    conn.commit()


print("Initial load completed.")