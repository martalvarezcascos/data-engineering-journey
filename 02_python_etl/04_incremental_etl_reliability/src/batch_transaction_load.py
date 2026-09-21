from pathlib import Path

import pandas as pd
import psycopg

from database import DB_CONFIG


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PROJECT_ROOT / "data" / "good_batch.csv"

df = pd.read_csv(CSV_PATH)

print(f"Rows in batch: {len(df)}")


with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:

    with conn.cursor() as cur:

        cur.execute("SELECT COUNT(*) FROM core.orders;")
        rows_before = cur.fetchone()[0]

        print(f"Rows before load: {rows_before}")

        try:

            with conn.transaction():

                for _, row in df.iterrows():

                    cur.execute(
                        """
                        INSERT INTO core.orders (
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

                    print(
                        f"Processed: {row['order_id']} "
                        f"line {row['line_id']}"
                    )

        except psycopg.errors.CheckViolation as exc:
            print("Batch failed.")
            print(f"Reason: {exc}")

        cur.execute("SELECT COUNT(*) FROM core.orders;")
        rows_after = cur.fetchone()[0]

        print(f"Rows after load: {rows_after}")
        print(f"Database unchanged: {rows_before == rows_after}")