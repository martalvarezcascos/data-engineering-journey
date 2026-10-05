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
    .appName("groupby-transformations-dataframe")
    .master("local[*]")
    .getOrCreate()
)


data_sales = [
    ("O3001", "P01", "S01", 2, 10.0),
    ("O3002", "P01", "S01", 1, 10.0),
    ("O3003", "P02", "S01", 3, 5.0),
    ("O3004", "P02", "S02", 4, 5.0),
    ("O3005", "P03", "S02", 2, 20.0),
]

schema_sales = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("store_id", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
])

df_sales = spark.createDataFrame(data_sales, schema=schema_sales)

df_total_revenue = df_sales.withColumn(
    "total_revenue",
    F.col("quantity")*F.col("unit_price")
)

df_store_summary = (
    df_total_revenue
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("number_of_orders"),
        F.sum("total_revenue").alias("total_revenue")
    )
) 

#--------------------------
df_store_product = (
    df_total_revenue
    .groupBy("store_id", "product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("number_of_orders"),
        F.sum("total_revenue").alias("total_revenue")
    )
)
#----------------------------

data_products = [
    ("P01", "Laptop Stand"),
    ("P02", "Mouse"),
    ("P03", "Keyboard"),
]

data_products_bad = [
    ("P01", "Laptop Stand"),
    ("P02", "Mouse"),
    ("P02", "Wireless Mouse"),
    ("P03", "Keyboard"),
]

schema_products = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
])

df_products = spark.createDataFrame(
    data_products_bad,
    schema=schema_products
)

df_check_duplicates = (
    df_products
    .groupBy("product_id")
    .agg(
        F.count("*").alias("count")
    )
)

df_keep_non_duplicates = (
    df_check_duplicates
    .filter(F.col("count") == 1)
)


df_enriched = (
    df_store_product
    .join(
        df_products,
        on="product_id",
        how="left"
    )
)

#-------------------------------------
data_sales_extra = [
    ("O3001", "P01", "S01", 2, 10.0),
    ("O3002", "P01", "S01", 1, 10.0),
    ("O3003", "P02", "S01", 3, 5.0),
    ("O3004", "P02", "S02", 4, 5.0),
    ("O3005", "P03", "S02", 2, 20.0),
    ("O3006", "P99", "S02", 1, 50.0),
]

df_sales_extra = spark.createDataFrame(data_sales_extra, schema=schema_sales)

df_orphan_products = (
    df_sales_extra
    .join(
        df_products,
        on="product_id",
        how="left_anti"
    )
)


#df_store_summary.show()
#df_store_product.show()
#df_orphan_products.show() 

#------------------------------------

#df_store_summary.explain()

#print(df_sales.rdd.getNumPartitions())

'''df_sales.withColumn(
    "partition_id",
    F.spark_partition_id()
).show()
'''
#--------------------------------------------

#df_narrow = df_sales.filter(F.col("quantity") > 1)

#df_wide = df_sales.select("store_id").distinct()

#df_narrow.explain()
#df_wide.explain()

#--------------------------------------------
df_repartitioned = df_sales.repartition(2)
df_coalesced = df_sales.coalesce(2)

print("Original:", df_sales.rdd.getNumPartitions())
print("Repartition:", df_repartitioned.rdd.getNumPartitions())
print("Coalesce:", df_coalesced.rdd.getNumPartitions())

df_repartitioned.explain()
df_coalesced.explain()