import psycopg

from database import DB_CONFIG


INSERT_NEW = """
INSERT INTO core.orders (
    order_id,
    line_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    unit_price,
    updated_at
)
SELECT
    s.order_id,
    s.line_id,
    s.order_date,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.updated_at
FROM staging.orders AS s
LEFT JOIN core.orders AS c
    ON s.order_id = c.order_id
   AND s.line_id = c.line_id
WHERE c.order_id IS NULL;
"""


UPDATE_EXISTING = """
UPDATE core.orders AS c
SET
    order_date = s.order_date,
    customer_id = s.customer_id,
    product_id = s.product_id,
    quantity = s.quantity,
    unit_price = s.unit_price,
    updated_at = s.updated_at
FROM staging.orders AS s
WHERE c.order_id = s.order_id
  AND c.line_id = s.line_id
  AND s.updated_at > c.updated_at;
"""


with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:

    with conn.cursor() as cur:

        try:
            with conn.transaction():

                cur.execute(INSERT_NEW)
                inserted_rows = cur.rowcount

                cur.execute(UPDATE_EXISTING)
                updated_rows = cur.rowcount

            print("Incremental load completed.")
            print(f"Inserted rows: {inserted_rows}")
            print(f"Updated rows: {updated_rows}")

        except Exception as exc:
            print("Incremental load failed.")
            print(f"Reason: {exc}")
            raise