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
    TimestampType,
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("week6-window-deduplication")
    .getOrCreate()
)

catalog_history_schema = StructType([
    StructField("market", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("updated_at", StringType(), False),
])

catalog_history_data = [
    ("ES", "P01", "Soporte portátil", "Accesorios", "2026-09-20 10:00:00"),
    ("ES", "P01", "Soporte portátil Pro", "Accesorios", "2026-10-01 09:00:00"),
    ("ES", "P02", "Ratón inalámbrico", "Accesorios", "2026-09-25 15:00:00"),
    ("PT", "P01", "Suporte portátil", "Acessórios", "2026-09-28 12:00:00"),
    ("ES", "P01", "Soporte portátil Ultra", "Accesorios", "2026-10-01 09:00:00"),
]

df_catalog_history = spark.createDataFrame(catalog_history_data, catalog_history_schema)
df_catalog_history = (df_catalog_history.withColumn(
                                                    "updated_at",
                                                    F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss") 
                                                    ))

#-------------------------------
window_spec = (Window
               .partitionBy('market','product_id')
               .orderBy(F.col('updated_at').desc())
               )

df_ranked = (
    df_catalog_history.withColumn(
        "row_number",
        F.row_number().over(window_spec)
    )
)

#-------------------------------------
df_catalog_current = df_ranked.filter(F.col('row_number')==1)

#--------------------------------------
df_same_update = (
    df_catalog_history
    .groupBy("market", "product_id", "updated_at").agg(
        F.count("*").alias("count")
    ).filter(F.col("count") > 1)
)

#-------------------------------------
history_window = (
    Window
    .partitionBy('market','product_id')
    .orderBy(F.col('updated_at').asc())
)

df_with_history = (
    df_catalog_history
    .withColumn(
        "previous_product_name",
        F.lag("product_name").over(history_window)
    )
    .withColumn(
        "previous_category",
        F.lag("category").over(history_window)
    )
)

#-----------------------------------------
df_with_history = df_with_history.withColumn(
    "product_name_changed",
    F.when(F.col("product_name") != F.col("previous_product_name"), True)
    .when(F.col("previous_product_name").isNull(), False)
    .otherwise(False)
)

df_with_history = df_with_history.withColumn(
    "category_changed",
    F.when(F.col("category") != F.col("previous_category"), True)
    .when(F.col("previous_category").isNull(), False)
    .otherwise(False)
)

#-----------------------------------------

ranking_window = (
    Window
    .partitionBy('market','product_id')
    .orderBy(F.col('updated_at').desc())
)

df_rankings = (
    df_catalog_history
    .withColumn("row_number", F.row_number().over(ranking_window))
    .withColumn("rank", F.rank().over(ranking_window))
    .withColumn("dense_rank", F.dense_rank().over(ranking_window))
)

df_rankings.show(truncate=False)