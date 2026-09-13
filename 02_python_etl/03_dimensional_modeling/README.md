# Dimensional Modeling — Retail Sales Star Schema

## Overview

This project builds a small dimensional data model for retail sales using Python, pandas and SQL.

The goal is to transform raw transactional data into an analytics-ready **star schema**, applying core Data Engineering concepts such as:

* fact and dimension tables
* grain definition
* surrogate and business keys
* many-to-one dimension lookups
* dimensional data quality checks
* analytical SQL with aggregations and window functions

The pipeline reads raw CSV files, transforms the data, builds the dimensional model, validates its integrity and writes the processed tables to disk.

---

## Project Structure

```text
03_dimensional_modeling/
│
├── data/
│   ├── raw/
│   │   ├── sales.csv
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   └── stores.csv
│   │
│   └── processed/
│       ├── fact_sales.csv
│       ├── dim_customer.csv
│       ├── dim_product.csv
│       ├── dim_store.csv
│       └── dim_date.csv
│
├── src/
│   └── build_star_schema.py
│
├── sql/
│   └── analytical_queries.sql
│
└── README.md
```

---

## Data Model

The central fact table is `fact_sales`.

### Grain

One row in `fact_sales` represents **one product sold within an order**.

This grain determines which measures can safely be stored and aggregated in the fact table.

### Star Schema

```text
                  dim_product
                       |
                       |
dim_customer ---- fact_sales ---- dim_store
                       |
                       |
                   dim_date
```

### fact_sales

```text
order_id
date_key
customer_key
store_key
product_key
quantity
unit_price
discount_amount
gross_revenue
net_revenue
```

`order_id` is retained as a transactional identifier.

The fact table stores surrogate keys referencing the corresponding dimensions rather than the original source-system IDs.

### Dimensions

`dim_customer`

```text
customer_key
customer_id
customer_name
city
country
```

`dim_product`

```text
product_key
product_id
product_name
category
brand
```

`dim_store`

```text
store_key
store_id
store_name
store_city
```

`dim_date`

```text
date_key
full_date
day
month
month_name
quarter
year
day_of_week_num
day_of_week_name
is_weekend
```

The original IDs such as `customer_id`, `product_id` and `store_id` are retained as **business keys**, while integer surrogate keys are generated for the dimensional model.

---

## Revenue Metrics

Two revenue measures are derived during transformation:

```text
gross_revenue = quantity × unit_price

net_revenue = gross_revenue - discount_amount
```

Both are stored at the same grain as the fact table.

---

## Pipeline

The Python pipeline follows this flow:

```text
Extract
   ↓
Transform
   ↓
Build Dimensions
   ↓
Build Fact Table
   ↓
Validate Model
   ↓
Load Processed Data
```

### Extract

The pipeline loads the raw CSV datasets using pandas.

### Transform

The transformation stage:

* converts `order_date` to datetime
* calculates `gross_revenue`
* calculates `net_revenue`

### Build Dimensions

Separate dimension tables are created for customers, products, stores and dates.

Surrogate keys are generated for each dimension.

### Build Fact Table

The raw sales data is enriched with surrogate keys using pandas `merge()` operations.

Dimension lookups are validated as `many_to_one` relationships to prevent accidental row multiplication.

The original business keys are then removed from the fact table and replaced with the corresponding surrogate keys.

---

## Data Quality

The pipeline performs several validations before writing the processed model.

These include:

```text
Foreign keys exist in their corresponding dimensions

Foreign keys contain no NULL values

Dimension primary keys are unique and non-null

Dimension business keys are unique and non-null

quantity > 0

unit_price >= 0

discount_amount >= 0

gross_revenue >= 0

net_revenue >= 0

gross_revenue = quantity × unit_price

net_revenue = gross_revenue - discount_amount

The number of fact rows does not change during dimension lookups
```

The current dataset also assumes that the combination:

```text
order_id + product_key
```

uniquely identifies a sales line.

In a production transactional system, an explicit `order_line_id` would be preferable if the same product could appear multiple times as separate lines within one order.

---

## Analytical SQL

The dimensional model is used to answer several analytical questions in `sql/analytical_queries.sql`.

The queries include:

```text
Monthly net revenue

Net revenue by product category

Top 3 products by revenue for each store

Daily store revenue compared with the previous available day

Cumulative revenue by store
```

The queries combine dimensional joins, aggregations, CTEs and SQL window functions such as:

```sql
ROW_NUMBER()
LAG()
SUM() OVER()
```

---

## How to Run

From the project directory:

```bash
python src/build_star_schema.py
```

The script reads the files from:

```text
data/raw/
```

and writes the validated dimensional model to:

```text
data/processed/
```

If a data quality or referential-integrity validation fails, the pipeline raises an exception instead of publishing invalid processed data.

---

## Key Learnings

This project focuses on the transition from working with flat datasets to designing data specifically for analytical workloads.

The main concepts practiced are:

```text
Defining and protecting table grain
Separating facts from dimensions
Using surrogate vs business keys
Building a star schema
Managing many-to-one dimension lookups
Preventing row multiplication during joins
Applying data quality rules
Using dimensional models with analytical SQL
Combining GROUP BY, CTEs and window functions
```

---

## Possible Next Steps

Future iterations could extend the project with:

* incremental loads
* persistent surrogate-key management
* Slowly Changing Dimensions Type 2
* database or warehouse persistence instead of CSV outputs
* automated tests
* orchestration
* implementation in Databricks / Spark

