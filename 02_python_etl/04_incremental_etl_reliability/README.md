# Incremental ETL Reliability

A hands-on Data Engineering project focused on persistence, transactional integrity, incremental loading, idempotency and automated data-quality testing using Python, PostgreSQL and Docker.

## Project goals

This project explores how to build a reliable incremental pipeline that:

- persists data in PostgreSQL;
- separates incoming data from validated core data;
- enforces data integrity through database constraints;
- processes new and updated records incrementally;
- prevents older versions from overwriting newer data;
- handles late-arriving records;
- deduplicates multiple versions of the same business key;
- remains safe when the same batch is processed repeatedly;
- uses transactions to guarantee atomicity;
- validates pipeline guarantees automatically with pytest.

## Architecture

```text
CSV source
    ↓
Python / pandas
    ↓
staging.orders
    ↓
deduplication and version selection
    ↓
incremental UPSERT
    ↓
core.orders
```

PostgreSQL runs locally inside Docker and stores its database files in a persistent Docker volume.

## Data grain

The grain of `orders` is:

> One row represents one order line.

The business key is therefore:

```text
(order_id, line_id)
```

The same product can appear more than once within the same order, so `(order_id, product_id)` is not considered unique.

## Staging and core

### `staging.orders`

Represents the current incoming batch.

It is intentionally more permissive because it acts as the landing area for source data.

### `core.orders`

Represents the validated persistent state.

Important constraints include:

```sql
PRIMARY KEY (order_id, line_id)

CHECK (quantity > 0)

CHECK (unit_price >= 0)
```

Required fields are also protected with `NOT NULL`.

## Transactions

Multi-step operations are executed atomically.

A batch is treated as one logical unit:

```text
BEGIN
    operation 1
    operation 2
    operation 3
COMMIT
```

If any operation fails:

```text
ROLLBACK
```

This prevents partially loaded batches from leaving the database in an inconsistent state.

Python transaction boundaries are managed using Psycopg transaction contexts.

## Incremental loading

Each row contains an `updated_at` timestamp.

Incoming rows are classified according to their business key and version:

```text
Key does not exist
→ INSERT

Key exists and incoming.updated_at > core.updated_at
→ UPDATE

Key exists and incoming.updated_at <= core.updated_at
→ NO ACTION
```

`order_date` is not used to determine the latest version because old business events can still arrive or be modified later.

## Deduplication

A batch may contain multiple versions of the same business key.

The latest version is selected using:

```sql
ROW_NUMBER() OVER (
    PARTITION BY order_id, line_id
    ORDER BY updated_at DESC
)
```

Only `rn = 1` is passed to the incremental load.

If two conflicting records have the same `updated_at`, an additional deterministic source sequence or version field would be required.

## UPSERT

PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` is used to combine insert and update behavior.

Updates are protected by:

```sql
WHERE EXCLUDED.updated_at > core.orders.updated_at
```

This prevents stale records from overwriting newer versions.

UPSERT is used here because the project models a relatively small current-state table with a clear primary key. Other storage engines and larger analytical workloads may require different strategies.

## Idempotency

The pipeline is designed so that processing the same batch repeatedly does not change the final state after the first successful execution.

Example:

```text
First execution
30 → 31 rows

Same batch again
31 → 31 rows
```

This makes pipeline retries safe.

## Late-arriving data

A record can have an old `order_date` but still be new to the pipeline.

For example:

```text
order_date = 2026-08-30
updated_at = 2026-09-19
```

If its business key does not yet exist, it is inserted normally.

This demonstrates why event date and ingestion/version timestamps serve different purposes.

## Automated tests

Pytest integration tests validate important pipeline invariants:

- no duplicate `(order_id, line_id)` keys;
- mandatory fields are not null;
- business rules remain valid;
- processing the same batch twice is idempotent;
- older versions cannot overwrite newer data;
- late-arriving new business keys are inserted correctly.

Tests run against PostgreSQL but use transaction rollback so test data does not persist after execution.

Run the suite with:

```bash
python -m pytest -v
```

## Technology

- Python 3.11
- pandas
- Psycopg 3
- PostgreSQL 17
- Docker / Docker Compose
- pytest

## Running locally

Create a local environment file based on:

```text
.env.example
```

Start PostgreSQL:

```bash
docker compose up -d
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Run tests:

```bash
python -m pytest -v
```

## Key learnings

This project demonstrates that reliable Data Engineering requires more than moving data from one place to another.

The pipeline must explicitly define:

- row grain;
- business keys;
- data-quality rules;
- transaction boundaries;
- version precedence;
- incremental behavior;
- retry behavior;
- and testable guarantees.