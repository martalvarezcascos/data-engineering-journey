import psycopg
import pytest
from datetime import datetime
from uuid import uuid4

from src.database import DB_CONFIG
from src.metadata import start_run, finish_run_success

@pytest.fixture
def db_conn():
    
    conn = psycopg.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def test_cycle(db_conn):
    run_id = str(uuid4())
    started_at = datetime.now()
    start_run(db_conn, run_id, started_at)

    # Check that the run has been started correctly by checking  control.pipeline_runs
    with db_conn.cursor() as cur:
        cur.execute("SELECT status FROM control.pipeline_runs WHERE run_id = %s", (run_id,))
        result = cur.fetchone()
        assert result is not None
        assert result[0] == "RUNNING"
    
    finished_at = datetime.now()
    staged_rows = 10
    affected_rows = 5
    finish_run_success(db_conn, run_id, finished_at, staged_rows, affected_rows)

    # Check that the run has been finished correctly by checking control.pipeline_runs
    with db_conn.cursor() as cur:
        cur.execute("SELECT status, finished_at, staged_rows, affected_rows FROM control.pipeline_runs WHERE run_id = %s", (run_id,))
        result = cur.fetchone()
        assert result is not None
        assert result[0] == "SUCCESS"
        assert result[1] == finished_at
        assert result[2] == staged_rows
        assert result[3] == affected_rows