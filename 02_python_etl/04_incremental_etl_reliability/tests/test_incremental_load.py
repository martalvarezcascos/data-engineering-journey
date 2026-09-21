import psycopg
import pytest

from src.database import DB_CONFIG
from src.upsert_incremental import apply_upsert

@pytest.fixture
def db_conn():
    
    conn = psycopg.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def test_incremental_load_is_idempotent(db_conn):

    with db_conn.cursor() as cur:

        # Dejamos libre nuestra clave artificial de prueba
        cur.execute(''' DELETE FROM core.orders WHERE order_id = 'T9001'; ''')

        # Staging representará exclusivamente nuestro batch de prueba
        cur.execute(''' TRUNCATE TABLE staging.orders; ''')

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
            [
                (
                    "T9001",
                    1,
                    "2026-09-21",
                    "C_TEST",
                    "P001",
                    2,
                    12.50,
                    "2026-09-21 10:00:00",
                ),
                (
                    "T9001",
                    1,
                    "2026-09-21",
                    "C_TEST",
                    "P001",
                    5,
                    12.50,
                    "2026-09-21 11:00:00",
                ),
            ],
        )

    # Primera ejecución
    first_run = apply_upsert(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT quantity
            FROM core.orders
            WHERE order_id = 'T9001'
              AND line_id = 1;
            """
        )
        quantity_after_first_run = cur.fetchone()[0]

    # Exactamente el mismo batch otra vez
    second_run = apply_upsert(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*),
                MAX(quantity)
            FROM core.orders
            WHERE order_id = 'T9001'
              AND line_id = 1;
            """
        )

        row_count, quantity_after_second_run = cur.fetchone()

    assert first_run == 1
    assert quantity_after_first_run == 5

    assert second_run == 0
    assert row_count == 1
    assert quantity_after_second_run == 5


def test_older_version_does_not_overwrite_core(db_conn):

    with db_conn.cursor() as cur:

        cur.execute(
            """
            DELETE FROM core.orders
            WHERE order_id = 'T9002';
            """
        )

        cur.execute("TRUNCATE TABLE staging.orders;")

        # Estado actual de core: versión reciente
        cur.execute(
            """
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                "T9002",
                1,
                "2026-09-20",
                "C_TEST",
                "P002",
                5,
                8.75,
                "2026-09-21 12:00:00",
            ),
        )

        # Staging trae una versión más antigua
        cur.execute(
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
            (
                "T9002",
                1,
                "2026-09-20",
                "C_TEST",
                "P002",
                999,
                8.75,
                "2026-09-21 10:00:00",
            ),
        )

    affected_rows = apply_upsert(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                quantity,
                updated_at
            FROM core.orders
            WHERE order_id = 'T9002'
              AND line_id = 1;
            """
        )

        quantity, updated_at = cur.fetchone()

    assert affected_rows == 0
    assert quantity == 5
    assert str(updated_at) == "2026-09-21 12:00:00"


def test_late_arriving_new_key_is_inserted(db_conn):

    with db_conn.cursor() as cur:

        cur.execute(
            """
            DELETE FROM core.orders
            WHERE order_id = 'T9003';
            """
        )

        cur.execute("TRUNCATE TABLE staging.orders;")

        cur.execute(
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
            (
                "T9003",
                1,
                "2026-08-01",
                "C_TEST",
                "P003",
                2,
                22.00,
                "2026-09-21 13:00:00",
            ),
        )

    affected_rows = apply_upsert(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                order_date,
                quantity
            FROM core.orders
            WHERE order_id = 'T9003'
              AND line_id = 1;
            """
        )

        row = cur.fetchone()

    assert affected_rows == 1
    assert row is not None
    assert str(row[0]) == "2026-08-01"
    assert row[1] == 2