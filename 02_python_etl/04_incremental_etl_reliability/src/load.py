import pandas as pd
import psycopg
from src.database import DB_CONFIG
import logging

logger = logging.getLogger(__name__)

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


def load_staging(conn, df) -> int:

    rows = list(df.itertuples(index=False, name=None))

    with conn.cursor() as cur:

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

    logger.info("Loaded %s rows into staging", len(rows))
    return len(rows)



def apply_upsert(conn) -> int:

    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL)
        affected_rows = cur.rowcount
        logger.info("UPSERT affected %s rows", affected_rows)
        return affected_rows