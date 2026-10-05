import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("first-spark-dataframe")
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

schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
])


data = [
    ("O1001", "P01", 2, 10.5),
    ("O1002", "P02", 5, 7.0),
    ("O1003", "P01", 1, 10.5),
]

df = spark.createDataFrame(data, schema=schema)

df_filtered = df.filter(F.col("quantity") > 1)

df_enriched = df_filtered.withColumn(
    "total_amount",
    F.col("quantity") * F.col("unit_price")
)

df_enriched.show()


spark.stop()