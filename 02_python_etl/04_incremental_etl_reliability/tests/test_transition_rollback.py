import pandas as pd
import psycopg
import pytest

from src.database import DB_CONFIG
from src.load import load_staging

@pytest.fixture
def db_conn():
    
    conn = psycopg.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def test_transition_rollback(db_conn):

    # Creamos dataframe de prueba
    df = pd.DataFrame({
        "order_id": ["T9001"],
        "line_id": [1],
        "order_date": ["2026-09-21"],
        "customer_id": ["C_TEST"],
        "product_id": ["P001"],
        "quantity": [5],
        "unit_price": [12.50],
        "updated_at": ["2026-09-21 11:00:00"]
    })

    # Creamos dataframe de actualización (simulando un cambio en el batch de prueba)
    df_new = pd.DataFrame({
            "order_id": ["T9001"],
            "line_id": [1],
            "order_date": ["2026-09-21"],
            "customer_id": ["C_TEST"],
            "product_id": ["P001"],
            "quantity": [10],
            "unit_price": [14.50],
            "updated_at": ["2026-09-21 11:00:00"]
    })

    # Cargamos el dataframe en la tabla staging
    load_staging(db_conn, df)
    # Commit
    db_conn.commit()

    with pytest.raises(RuntimeError):
        with db_conn.transaction():
            load_staging(db_conn, df_new)
            raise RuntimeError("Forced test failure")


    # Verificamos que los datos en la tabla staging no se hayan actualizado debido al rollback
    with db_conn.cursor() as cur:
        cur.execute("SELECT quantity, unit_price FROM staging.orders WHERE order_id = 'T9001' AND line_id = 1")
        result = cur.fetchone()
        assert result == (5, 12.50)