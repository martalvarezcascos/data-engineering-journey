import psycopg

from src.database import DB_CONFIG


UPSERT_SQL = """
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, line_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM staging.orders
),

latest AS (
    SELECT
        order_id,
        line_id,
        order_date,
        customer_id,
        product_id,
        quantity,
        unit_price,
        updated_at
    FROM ranked
    WHERE rn = 1
)

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
    order_id,
    line_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    unit_price,
    updated_at
FROM latest

ON CONFLICT (order_id, line_id)

DO UPDATE SET
    order_date = EXCLUDED.order_date,
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    updated_at = EXCLUDED.updated_at

WHERE EXCLUDED.updated_at > core.orders.updated_at;
"""

def apply_upsert(conn):
    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL)
        return cur.rowcount


def main():
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        with conn.transaction():
            affected_rows = apply_upsert(conn)

        print("UPSERT completed.")
        print(f"Rows affected: {affected_rows}")

if __name__ == '__main__':
    main()