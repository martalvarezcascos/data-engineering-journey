# 05 — PySpark Fundamentals

## Objective

Build a solid foundation in Apache Spark and PySpark by understanding how distributed processing differs from local pandas-based workflows, and by implementing a small end-to-end PySpark pipeline with explicit schemas, transformations, joins, aggregations, data quality checks, and execution-plan awareness.

---

## Environment

- Python 3.11
- Java 17
- PySpark 4.2
- Local Spark mode: `local[*]`
- Virtual environment: `.venv-spark`

Because the project runs locally on Windows, the scripts explicitly set:

```python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

This ensures that Spark workers use the same Python interpreter as the active virtual environment.

---

## Concepts Covered

### Spark fundamentals

- Spark vs PySpark
- Driver, executors, and partitions
- Local Spark execution
- Lazy evaluation
- Transformations vs actions

### DataFrames

- `SparkSession`
- Explicit schemas with `StructType` and `StructField`
- Schema inference vs explicit contracts
- `select`
- `filter`
- `withColumn`
- `alias`
- `withColumnRenamed`
- `drop`
- `cast`

### Null handling and conditional logic

- `isNull`
- `isNotNull`
- `na.fill`
- `when`
- `otherwise`
- Null propagation in expressions

### Aggregations

- `groupBy`
- `agg`
- `sum`
- `count`
- Grain changes after aggregation

### Joins

- `inner`
- `left`
- `left_semi`
- `left_anti`
- Join cardinality
- Grain preservation
- Duplicate dimension keys and metric inflation

### Spark execution

- `explain()`
- Physical plans
- `Exchange`
- Shuffle
- Narrow vs wide transformations
- Partitions
- `spark_partition_id()`
- `repartition()`
- `coalesce()`

---

## Mini PySpark Pipeline

The mini pipeline follows this structure:

```text
raw sales
   ↓
explicit schema
   ↓
null validation
   ↓
valid / invalid split
   ↓
referential integrity validation
   ↓
left_anti → orphan records
left_semi → valid records
   ↓
business transformations
   ↓
dimension enrichment
   ↓
aggregation
   ↓
reconciliation checks
   ↓
output
```

---

## Data Quality Decisions

The pipeline intentionally avoids silently replacing missing or invalid values.

Examples:

- Rows with `quantity = NULL` are separated instead of being automatically converted to `0`.
- Product IDs that do not exist in the product dimension are detected with a `left_anti` join.
- Valid sales are selected with a `left_semi` join.
- Join cardinality is considered before enrichment to avoid duplicated rows and inflated metrics.
- Aggregated order counts are reconciled against the number of valid input rows.

The main principle is:

> Missing information should not be converted into invented business meaning.

---

## Grain

Grain is explicitly considered throughout the pipeline.

Examples:

```text
Raw sales
→ 1 row per order

Store-product aggregation
→ 1 row per (store_id, product_id)

Store summary
→ 1 row per store_id
```

A join is only considered safe when the cardinality of the lookup table preserves the expected grain.

---

## Example Aggregation

```python
df_store_summary = (
    df_enriched_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_revenue").alias("total_revenue")
    )
)
```

---

## Reconciliation

The pipeline validates that the number of valid sales matches the number of orders represented in the final aggregation.

```python
valid_count = df_valid_sales.count()

total_orders = (
    df_store_summary
    .agg(F.sum("total_orders").alias("total_orders"))
    .collect()[0]["total_orders"]
)
```

For this small validation, `collect()` is acceptable because only a single aggregated scalar is returned to the driver.

---

## Spark Execution and Performance

Execution plans were inspected using:

```python
df.explain()
```

Important observations:

### Narrow transformations

Examples:

```text
filter
select
withColumn
```

These can normally be processed independently within each partition.

### Wide transformations

Examples:

```text
groupBy
distinct
repartition
```

These may require redistribution of data between partitions.

A shuffle is visible in the physical plan through an `Exchange`, for example:

```text
Exchange hashpartitioning(...)
```

---

## Partitions

Partitions are logical units of work, not machines or executors.

Example:

```python
df_sales.rdd.getNumPartitions()
```

and:

```python
df_sales.withColumn(
    "partition_id",
    F.spark_partition_id()
)
```

were used to inspect how rows are distributed.

---

## Repartition vs Coalesce

### `repartition()`

- Can increase or decrease partitions.
- Redistributes data.
- Usually involves a shuffle.
- Produces a more balanced distribution.

### `coalesce()`

- Mainly used to reduce partitions.
- Tries to avoid a full shuffle.
- Cheaper, but may produce less balanced partitions.

Example:

```text
repartition → more expensive, more balanced
coalesce    → cheaper when reducing, less balanced
```

---

## Windows Local Environment Note

The local Windows environment produces Hadoop-related warnings such as:

```text
HADOOP_HOME and hadoop.home.dir are unset
winutils.exe not found
```

These warnings do not prevent in-memory Spark transformations.

However, local Parquet writing failed because Spark/Hadoop attempted to use Windows-specific filesystem utilities.

For this learning project, persistence to local Parquet was intentionally deferred rather than introducing an unofficial `winutils.exe` dependency.

Later exercises using Databricks and Delta Lake will provide a more realistic environment for persistence.

---

## How to Run

Activate the Spark virtual environment from the repository root:

```powershell
.\.venv-spark\Scripts\Activate.ps1
```

Move to the project folder:

```powershell
cd .\02_python_etl\05_pyspark_fundamentals
```

Run the scripts:

```powershell
python .\first_spark_session.py
```

or:

```powershell
python .\mini_spark_pipeline.py
```

---

## Key Learnings

- Spark is a distributed processing engine; PySpark is the Python API used to control it.
- Spark transformations are lazily evaluated.
- Actions trigger execution.
- Explicit schemas are safer than relying on automatic inference.
- Grain must be controlled before and after aggregations and joins.
- Join cardinality can silently duplicate rows and inflate metrics.
- `left_anti` and `left_semi` are useful tools for Data Quality.
- Wide transformations can trigger expensive shuffles.
- `Exchange` in an execution plan is an important signal of data redistribution.
- Partition count should be treated as an engineering decision, not a fixed rule.
- A technically successful Spark job can still produce incorrect business results if grain, cardinality, or data quality are not validated.