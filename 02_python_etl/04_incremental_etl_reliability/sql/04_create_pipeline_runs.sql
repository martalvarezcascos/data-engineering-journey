CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.pipeline_runs (
	run_id UUID PRIMARY KEY,
	started_at TIMESTAMP NOT NULL,
	finished_at TIMESTAMP NULL,
	status VARCHAR(20) NOT NULL,
	staged_rows INTEGER NULL,
	affected_rows INTEGER NULL,
	error_message TEXT NULL,

	CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))

	);