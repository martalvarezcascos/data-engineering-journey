import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("initial-transformations-dataframe")
    .master("local[*]")
    .getOrCreate()
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

data_nulls = [
    ("O2001", "P01", 2, 10.5),
    ("O2002", "P02", None, 7.0),
    ("O2003", None, 1, 12.0),
]

schema_nulls = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=False),
])

df_nulls = spark.createDataFrame(data_nulls, schema=schema_nulls)

df_quantity_not_null = df_nulls.filter(F.col("quantity").isNotNull())
df_product_null = df_nulls.filter(F.col("product_id").isNull())
df_quantity_filled = df_nulls.na.fill({"quantity": 0})


df_price_category = df_nulls.withColumn(
    "price_category",
    F.when(F.col("unit_price") >= 10, "high")
     .otherwise("low")
)

df_quantity_category = df_nulls.withColumn(
    "quantity_category",
    F.when(F.col("quantity") >= 2, "high")
     .when(F.col("quantity").isNull(), "missing")
     .otherwise("low")
)

df_quality_status = df_nulls.withColumn(
    "data_quality_status",
    F.when(F.col("quantity").isNull(), "invalid")
     .when(F.col("product_id").isNull(), "invalid")
     .otherwise("valid")
)

df_total_amount = df_quality_status.withColumn(
    "total_amount",
    F.col("quantity") * F.col("unit_price")
)


df_rename = df_price_category.withColumnRenamed("unit_price", "price")

df_clean = df_rename.drop("product_id")

df_final = df_total_amount.select(
    "order_id",
    "product_id",
    "quantity",
    "total_amount",
    "data_quality_status"
)



df_final.show()


spark.stop()