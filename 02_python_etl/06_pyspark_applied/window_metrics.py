import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("week6-window-metrics")
    .getOrCreate()
)

sales_schema = StructType([
    StructField("market", StringType(), False),
    StructField("sale_date", StringType(), False),
    StructField("amount", IntegerType(), False),
])

sales_data = [
    ("ES", "2026-10-01", 100),
    ("ES", "2026-10-02", 50),
    ("ES", "2026-10-03", 80),
    ("ES", "2026-10-04", 120),
    ("PT", "2026-10-01", 40),
    ("PT", "2026-10-03", 60),
    ("PT", "2026-10-04", 30),
]

df_sales = spark.createDataFrame(
    sales_data,
    schema=sales_schema
)

df_sales = df_sales.withColumn(
    "sale_date",
    F.to_date("sale_date", "yyyy-MM-dd")
)


cumulative_window = (
    Window
    .partitionBy('market')
    .orderBy(F.col('sale_date').asc())
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

df_sales_cumulative = (
    df_sales
    .withColumn(
        "cumulative_amount",
        F.sum("amount").over(cumulative_window)
    )
)

df_sales_cumulative.show(truncate=False)