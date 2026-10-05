import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

spark = (
    SparkSession.builder
    .appName("mini-spark-pipeline")
    .master("local[*]")
    .getOrCreate()
)


data_sales = [
    ("O4001", "P01", "S01", 2, 10.0),
    ("O4002", "P02", "S01", 3, 5.0),
    ("O4003", "P03", "S02", 1, 20.0),
    ("O4004", "P02", "S02", None, 5.0),
    ("O4005", "P99", "S01", 2, 15.0),
]

data_products = [
    ("P01", "Laptop Stand"),
    ("P02", "Mouse"),
    ("P03", "Keyboard"),
]

schema_sales = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("store_id", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=False),
])


schema_products = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
])

df_sales = spark.createDataFrame(data_sales, schema=schema_sales)
df_products = spark.createDataFrame(data_products, schema=schema_products)

df_sales.printSchema()
df_products.printSchema()

# Data validity before transformation
df_valid_quantity = df_sales.filter(F.col("quantity").isNotNull())
df_invalidad_quantity = df_sales.filter(F.col("quantity").isNull())

# See products that don't have a corresponding entry in the products table
df_orphan_product = df_valid_quantity.join(
    df_products,
    on="product_id",
    how="left_anti"
)

df_valid_sales = df_valid_quantity.join(
    df_products,
    on="product_id",
    how="left_semi"
)

# Transformation and enrichment

df_sales_revenue = df_valid_sales.withColumn(
    "total_revenue",
    F.col("quantity") * F.col("unit_price")
)

df_enriched_sales = df_sales_revenue.join(
    df_products,
    on="product_id",
    how="left"
)

df_store_summary = df_enriched_sales \
    .groupBy("store_id") \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_revenue").alias("total_revenue")
    )

# Validation
valid_count = df_valid_sales.count()
total_orders = df_store_summary.agg(F.sum("total_orders").alias("total_orders")).collect()[0]["total_orders"]

# Store
output_path = "output/store_summary"

(
    df_store_summary
    .write
    .mode("overwrite")
    .parquet(output_path)
)


# Check the stored parquet file
df_check = spark.read.parquet(output_path)
df_check.show()
df_check.printSchema()