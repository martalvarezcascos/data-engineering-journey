

def start_run(conn, run_id, started_at) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO control.pipeline_runs (
                    run_id, 
                    started_at, 
                    status
            )
            VALUES (%s, %s, 'RUNNING'); 
            """,
            (run_id, started_at)
        )

def finish_run_success(conn, run_id, finished_at, staged_rows, affected_rows,) -> None:
	with conn.cursor() as cur:
		cur.execute(
		"""
		UPDATE control.pipeline_runs SET
			status = 'SUCCESS',
			finished_at = %s,
			staged_rows = %s,
			affected_rows = %s,
			error_message = NULL
		WHERE run_id = %s;
        """,
        (finished_at, staged_rows, affected_rows, run_id)
        )



def finish_run_failure(conn, run_id, finished_at,
                       error_message,) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE control.pipeline_runs SET
                status = 'FAILED',
                finished_at = %s,
                error_message = %s
            WHERE run_id = %s;
            """,
            (finished_at, error_message, run_id)
        )