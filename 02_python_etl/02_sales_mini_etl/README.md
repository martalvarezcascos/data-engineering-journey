# Sales Mini ETL

Small ETL pipeline built with Python and pandas to process raw ecommerce sales data.

## Pipeline

The pipeline:

1. Extracts raw sales data from a CSV file.
2. Validates the expected schema.
3. Removes exact duplicates.
4. Cleans and standardizes text fields.
5. Converts quantities, prices, and dates to the appropriate data types.
6. Removes records that cannot be processed reliably.
7. Calculates revenue per order line.
8. Creates an aggregated sales summary by country and product category.
9. Loads both datasets as processed CSV files.

## Input

`data/raw/raw_sales.csv`

Each row represents one product line within an order.

## Outputs

`data/processed/clean_sales.csv`

Cleaned order-line level dataset including:

* standardized fields
* valid quantities and prices
* parsed order dates
* `total_amount = quantity * unit_price`

`data/processed/sales_summary.csv`

Aggregated by country and category with:

* number of unique orders
* units sold
* total revenue

## Run

From the repository root:

```bash
python 02_python_etl/02_sales_mini_etl/src/mini_etl.py
```

The pipeline logs its progress and fails explicitly when required input data is missing or invalid.
