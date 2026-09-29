# Incremental ETL Reliability

Small Data Engineering project focused on building a reliable, modular and observable incremental ETL pipeline with Python, PostgreSQL and Docker.

The project evolves a basic incremental load into a pipeline that supports:

- modular ETL architecture;
- data validation;
- staging and core layers;
- deduplication;
- PostgreSQL UPSERT;
- protection against older versions;
- idempotent reruns;
- transactional rollback;
- execution logging;
- pipeline run metadata;
- safe restartability;
- automated testing with pytest.

---

## Architecture

The pipeline follows this flow:

```text
CSV source
    ↓
extract
    ↓
validate
    ↓
staging.orders
    ↓
deduplication
    ↓
incremental UPSERT
    ↓
core.orders
```

Operational metadata is stored separately:

```text
control.pipeline_runs
```

Each execution receives a unique `run_id` and records its status and execution metadata.

---

## Project Structure

```text
04_incremental_etl_reliability/
│
├── data/
│
├── sql/
│   ├── 01_create_schemas.sql
│   ├── 02_create_core_orders.sql
│   ├── 03_load_core.sql
│   └── 04_create_pipeline_runs.sql
│
├── src/
│   ├── __init__.py
│   ├── database.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── metadata.py
│   └── main.py
│
├── tests/
│
├── .env.example
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Pipeline Modules

### `extract.py`

Reads the source CSV and returns a pandas DataFrame.

The extraction logic is isolated from database and transformation logic.

### `transform.py`

Validates the incoming dataset before loading.

The current validation checks that all required columns exist.

### `load.py`

Contains PostgreSQL loading logic.

Main responsibilities:

- truncate and load the current batch into `staging.orders`;
- deduplicate records using `ROW_NUMBER()`;
- apply the incremental load into `core.orders`;
- use PostgreSQL `INSERT ... ON CONFLICT`;
- update existing rows only when the incoming `updated_at` is newer.

The business key used by the pipeline is:

```text
(order_id, line_id)
```

### `metadata.py`

Manages execution metadata stored in:

```text
control.pipeline_runs
```

Each run stores:

- `run_id`;
- `started_at`;
- `finished_at`;
- `status`;
- `staged_rows`;
- `affected_rows`;
- `error_message`.

Supported statuses:

```text
RUNNING
SUCCESS
FAILED
```

### `main.py`

Acts as the orchestration layer.

It coordinates:

```text
extract
→ validate
→ load staging
→ incremental UPSERT
→ execution metadata
```

It also defines the transaction boundary for the data load.

---

## Incremental Load Logic

The pipeline first loads the full incoming batch into `staging.orders`.

Before loading into `core.orders`, records are deduplicated using:

```sql
ROW_NUMBER() OVER (
    PARTITION BY order_id, line_id
    ORDER BY updated_at DESC
)
```

Only the latest version of each business key is used.

The resulting rows are loaded with PostgreSQL UPSERT:

```sql
INSERT ...
ON CONFLICT (order_id, line_id)
DO UPDATE ...
```

Existing records are updated only when:

```sql
EXCLUDED.updated_at > core.orders.updated_at
```

This prevents older arriving versions from overwriting newer data already stored in `core`.

---

## Transactional Reliability

The staging load and incremental UPSERT are executed inside the same PostgreSQL transaction.

Conceptually:

```text
BEGIN

TRUNCATE staging
INSERT batch into staging
UPSERT staging → core

COMMIT
```

If any operation fails:

```text
ROLLBACK
```

This prevents partial loads.

For example, if staging is successfully loaded but the UPSERT fails, the staging changes are also rolled back.

---

## Idempotency

The pipeline is designed to be safely rerun with the same input.

If the same batch is processed again:

- existing rows are not duplicated;
- records with the same `updated_at` are not unnecessarily updated;
- older versions cannot overwrite newer versions.

Therefore, repeated execution of the same input produces the same final state in `core`.

Execution metadata is not idempotent by design: each execution receives a new `run_id`.

---

## Restartability

A failed pipeline run can be safely restarted from the beginning.

The strategy relies on:

```text
transactional atomicity
+
idempotent incremental loading
```

If a run fails:

```text
run A → FAILED
```

the cause can be corrected and the same batch can be executed again:

```text
run B → SUCCESS
```

without manually repairing staging or core tables.

---

## Logging

The pipeline uses Python's `logging` module instead of `print()` for operational visibility.

Logs include:

- timestamps;
- log level;
- module name;
- extracted row counts;
- staged row counts;
- affected UPSERT rows;
- successful validation;
- pipeline start and completion;
- error tracebacks.

Example:

```text
INFO | src.extract   | Extracted 4 rows
INFO | src.transform | Validation completed successfully
INFO | src.load      | Loaded 4 rows into staging
INFO | src.load      | UPSERT affected 3 rows
```

Exceptions are logged using `logger.exception()` so that the full traceback is preserved.

---

## Data Processing Failures vs Metadata Failures

The pipeline distinguishes between two different failure scenarios.

### ETL failure

If extraction, validation or database loading fails:

```text
data transaction → ROLLBACK
pipeline run      → FAILED
```

### Metadata failure after successful ETL

If the data transaction commits successfully but the final metadata update fails:

```text
data              → successfully committed
metadata          → may remain RUNNING
logs              → metadata-specific error
```

The pipeline does not incorrectly classify this scenario as a data-processing failure.

---

## Database Layers

The PostgreSQL database uses three schemas:

### `staging`

Temporary representation of the current incoming batch.

### `core`

Reliable persisted business data.

### `control`

Operational metadata about pipeline executions.

---

## Running the Pipeline

Start the PostgreSQL container if required:

```powershell
docker compose up -d
```

Run the pipeline from the project root:

```powershell
python -m src.main
```

---

## Running Tests

Run the full test suite:

```powershell
python -m pytest -q
```

Current test suite:

```text
9 tests
```

The tests cover:

- duplicate business keys;
- required fields;
- basic business rules;
- incremental idempotency;
- protection against older versions;
- late-arriving new keys;
- input validation;
- execution metadata;
- transactional rollback.

---

## Key Concepts Practised

This project consolidates several core Data Engineering concepts:

- grain and business keys;
- staging vs core data layers;
- incremental loading;
- business date vs update timestamp;
- late-arriving data;
- deduplication;
- window functions;
- PostgreSQL UPSERT;
- transactions;
- atomicity;
- rollback;
- idempotency;
- modular pipeline design;
- logging;
- operational metadata;
- restartability;
- automated integration testing.

---

## Technology Stack

- Python
- pandas
- PostgreSQL 17
- psycopg
- Docker
- Docker Compose
- pytest
- python-dotenv