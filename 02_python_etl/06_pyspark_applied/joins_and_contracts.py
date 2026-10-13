import os
import sys

# Mantener el mismo Python para el proceso principal y los workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("week6-joins-and-contracts")
    .getOrCreate()
)

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("line_id", IntegerType(), False),
    StructField("market", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
])

catalog_schema = StructType([
    StructField("market", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
])

orders_data = [
    ("O1001", 1, "ES", "P01", 2),
    ("O1001", 2, "ES", "P02", 1),
    ("O1002", 1, "PT", "P01", 3),
    ("O1003", 1, "ES", "P99", 1),
]

catalog_data = [
    ("ES", "P01", "Soporte para portátil", "Accesorios"),
    ("PT", "P01", "Suporte para portátil", "Acessórios"),
    ("ES", "P02", "Ratón inalámbrico", "Accesorios"),
    ("ES", "P01", "Soporte premium", "Accesorios"),
]

df_orders = spark.createDataFrame(orders_data, orders_schema)
df_catalog = spark.createDataFrame(catalog_data, catalog_schema)


df_enriched = df_orders.join(
    df_catalog,
    on = ["market", "product_id"],
    how = "left"
)

df_enriched = df_enriched.orderBy("order_id", "line_id")
#--------------------------------------

df_duplicates = (df_catalog
                 .groupBy("market", "product_id")
                 .agg(F.count("*").alias("row_count"))
)

df_duplicates_keys = df_duplicates.filter(F.col("row_count") > 1)

#--------------------------------------

df_catalog_valid = df_catalog.join(
    df_duplicates_keys,
    on=["market", "product_id"],
    how="left_anti"
)

#-------------------------------------------
df_enriched_valid = df_orders.join(
    df_catalog_valid,
    on = ["market", "product_id"],
    how = "left"
)

#-------------------------------------
df_duplicate_flag = (
    df_duplicates_keys
    .select("market", "product_id")
    .withColumn("is_catalog_duplicate", F.lit(True))
)

df_final = (
    df_enriched_valid
    .join(
        df_duplicate_flag,
        on=["market", "product_id"],
        how="left"
    )
)

df_final = df_final.withColumn(
    "enrichment_status",
    F.when(F.col("is_catalog_duplicate"), F.lit("CATALOG_DUPLICATE"))
     .when(F.col("product_name").isNull(), F.lit("CATALOG_MISSING"))
     .otherwise(F.lit("ENRICHED"))
)

#-------------------------------------

df_final.show(truncate=False)
 
valid_count = df_final.count()
total_orders = df_final.agg(F.sum("quantity").alias("total_quantity")).collect()[0]["total_quantity"]

print("Valid count:", valid_count)
print("Total orders:", total_orders)

row_count_per_status = df_final \
    .groupBy("enrichment_status") \
    .agg(F.count("*").alias("row_count")    
        )



row_count_per_status.show()

spark.stop()