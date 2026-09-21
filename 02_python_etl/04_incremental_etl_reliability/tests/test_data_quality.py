import psycopg

from src.database import DB_CONFIG

def test_core_has_no_duplicate_business_key():
    query = '''
            SELECT
                order_id,
                line_id,
                COUNT(*)
            FROM core.orders
            GROUP BY order_id, line_id
            HAVING COUNT(*) > 1; '''

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            duplicates = cur.fetchall()
    assert duplicates == []


def test_required_fields_are_not_null():
    query = '''
            SELECT COUNT(*)
            FROM core.orders
            WHERE 
                order_id IS NULL
                OR line_id IS NULL
                OR order_date IS NULL
                OR product_id IS NULL
                OR quantity IS NULL
                OR unit_price IS NULL
                OR updated_at IS NULL;
                 '''

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            cur.execute(query)
            invalid_rows = cur.fetchone()[0]
    assert invalid_rows == 0


def test_business_rules():

    query = '''
        SELECT COUNT(*)
        FROM core.orders
        WHERE
            quantity <= 0 OR
            unit_price < 0; '''

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            cur.execute(query)
            invalid_rows = cur.fetchone()[0]
            
    assert invalid_rows == 0
