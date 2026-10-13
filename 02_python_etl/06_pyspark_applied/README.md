# Week 6 — Applied PySpark: Data Quality, Window Functions & Pipeline Integration

## Project Overview

This project applies PySpark fundamentals to a small, end-to-end Data Engineering pipeline that processes order lines and historical product catalog data.

The objective is to build reliable transformations while preserving data integrity, handling invalid records, resolving historical product versions and validating analytical outputs.

All datasets are generated in memory for learning purposes.

## Project Structure

| Script | Purpose |
|---|---|
| `joins_and_contracts.py` | Composite-key joins, cardinality and catalog quality checks |
| `window_deduplication.py` | Window functions and deterministic version selection |
| `window_metrics.py` | Ranking, temporal analysis and cumulative aggregations |
| `dates_and_robust_transformations.py` | Date parsing, data validation and quarantine |
| `integrated_pipeline.py` | Integrated validation, catalog resolution, enrichment, aggregation and reconciliation |

## Integrated Pipeline Architecture

The integrated pipeline processes two sources:

**Order lines:** transactional records containing identifiers, markets, products, quantities, amounts and timestamps.

**Catalog history:** product information containing multiple historical versions identified by market and product.

The processing flow consists of:

1. Ingestion with explicit schemas.
2. Date and timestamp parsing.
3. Data Quality validation and quarantine.
4. Historical catalog ranking using window functions.
5. Detection and isolation of ambiguous current catalog versions.
6. Order enrichment using composite-key left joins.
7. Enrichment status classification.
8. Aggregation by market and enrichment status.
9. Data reconciliation and integrity checks.

## Grain and Data Contracts

**Order lines**

- Grain: one row per order line.
- Expected unique key: `(order_id, line_id)`.
- Product reference: `(market, product_id)`.
- Business dates and update timestamps must satisfy the defined input contract.

**Catalog history**

- Grain: one row per historical product version.
- Product business key: `(market, product_id)`.
- The most recent update determines the current version when the precedence rule produces one unambiguous candidate.
- Tied latest versions are quarantined when no valid business tie-breaker exists.

## Data Quality Strategy

Invalid order records are isolated rather than silently discarded.

The pipeline detects missing dates, incorrect date formats, invalid calendar dates, missing update timestamps and invalid timestamps.

Validation errors are stored in an array, allowing multiple independent issues to be associated with a single record.

Catalog issues are treated separately. If multiple versions share the latest timestamp, the affected product key is excluded from the current catalog.

Order enrichment distinguishes:

- `ENRICHED`: reliable catalog match.
- `CATALOG_DUPLICATE`: ambiguous current catalog version.
- `CATALOG_MISSING`: no available catalog match.

## Validation Results

Results obtained using the included sample dataset:

| Metric | Result |
|---|---:|
| Source order lines | 10 |
| Valid order lines | 7 |
| Quarantined order lines | 3 |
| Source amount | 605 |
| Valid amount | 490 |
| Quarantined amount | 115 |
| Valid catalog keys | 4 |
| Ambiguous catalog keys | 1 |
| Quarantined current catalog candidates | 2 |

The final analytical output contains 7 order lines, representing a total amount of 490 and a total quantity of 12.

The pipeline includes checks for row counts, financial reconciliation, quantity reconciliation, duplicate order-line keys and missing order-line keys after enrichment.

## Execution

Developed and exercised locally using:

- Python 3.11.9
- PySpark 4.2.0
- Java 17
- Windows and VS Code

The Spark dependency is documented separately in the repository's `requirements-spark.txt`.

From the repository root, using the configured `.venv-spark` environment:

```powershell
.\.venv-spark\Scripts\python.exe .\02_python_etl\06_pyspark_applied\integrated_pipeline.py
```

The project uses in-memory sample data and does not require external datasets.

## Spark Execution Concepts

The exercises cover lazy evaluation, narrow and wide transformations, shuffles, joins, window functions and aggregations.

Spark physical execution plans can be inspected using:

```python
df_market_quality_summary.explain(mode="formatted")
```

Repeated Spark actions may trigger recomputation. Execution performance and partition configuration remain areas for further investigation.

## Current Limitations

This is an educational pipeline, not a production deployment.

Current limitations include:

- In-memory sample datasets rather than external ingestion.
- No persistent output layer.
- No independent automated test suite.
- No structured production logging.
- No orchestration, scheduling or recovery mechanism.
- Incomplete validation of catalog input records.
- No authorized fallback policy for ambiguous current catalog versions.
- Performance not yet benchmarked or optimized for the local environment.

These limitations will inform future work with larger datasets, Databricks, Delta Lake and production-oriented pipeline design.

## Learning Outcomes

This project consolidates the ability to reason about data grain, business keys, cardinality, deterministic versioning, Data Quality, temporal transformations and reconciliation.

The primary emphasis is not simply producing correct output for sample data, but understanding the assumptions and checks required to make data transformations reliable.