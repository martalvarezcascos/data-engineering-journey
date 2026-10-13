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
    .master("local[2]")
    .appName("week6-integrated-pipeline")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# Para el ejercicio suponemos que los timestamps
# recibidos están expresados en UTC.

# ====================================================
# SOURCE A: ORDER LINES
# ====================================================

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("line_id", IntegerType(), False),
    StructField("market", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("line_amount", IntegerType(), False),
    StructField("order_date_raw", StringType(), True),
    StructField("updated_at_raw", StringType(), True),
])

orders_data = [
    ("O2001", 1, "ES", "P01", 2, 100,
     "2026-10-01", "2026-10-01 09:15:00"),

    ("O2001", 2, "ES", "P02", 1, 40,
     "2026-10-01", "2026-10-01 09:15:00"),

    ("O2002", 1, "PT", "P01", 3, 90,
     "2026-10-02", "2026-10-02 11:30:00"),

    ("O2003", 1, "ES", "P99", 1, 25,
     "2026-10-03", "2026-10-03 10:00:00"),

    ("O2004", 1, "ES", "P03", 2, 120,
     "2026-10-04", "2026-10-04 12:20:00"),

    ("O2005", 1, "ES", "P01", 1, 80,
     "2026-02-30", "2026-10-05 08:30:00"),

    ("O2006", 1, "ES", "P02", 1, 20,
     "2026-10-06", "not_a_timestamp"),

    ("O2007", 1, "PT", "P02", 2, 70,
     "2026-10-07", "2026-10-07 14:00:00"),

    ("O2008", 1, "ES", "P03", 1, 45,
     "2026-10-08", "2026-10-08 16:10:00"),

    ("O2009", 1, "PT", "P02", 1, 15,
     "09/10/2026", "2026-10-09 09:00:00"),
]

df_orders_raw = spark.createDataFrame(
    orders_data,
    schema=orders_schema
)

# ====================================================
# SOURCE B: CATALOG HISTORY
# ====================================================

catalog_schema = StructType([
    StructField("market", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("updated_at_raw", StringType(), False),
])

catalog_data = [
    ("ES", "P01", "Soporte portátil",
     "Accesorios", "2026-09-20 10:00:00"),

    ("ES", "P01", "Soporte portátil Pro",
     "Accesorios", "2026-10-01 09:00:00"),

    ("ES", "P02", "Ratón inalámbrico",
     "Accesorios", "2026-09-25 15:00:00"),

    ("PT", "P01", "Suporte portátil",
     "Acessórios", "2026-09-28 12:00:00"),

    ("PT", "P02", "Rato sem fios",
     "Acessórios", "2026-09-30 10:00:00"),

    ("ES", "P03", "Base ergonómica",
     "Accesorios", "2026-09-10 08:00:00"),

    ("ES", "P03", "Base ergonómica Pro",
     "Accesorios", "2026-10-01 08:00:00"),

    ("ES", "P03", "Base ergonómica Premium",
     "Accesorios", "2026-10-01 08:00:00"),
]

df_catalog_history = spark.createDataFrame(
    catalog_data,
    schema=catalog_schema
)

# ====================================================
# INITIAL INSPECTION
# ====================================================

#print("ORDERS")
#df_orders_raw.show(truncate=False)
#df_orders_raw.printSchema()

#print("CATALOG HISTORY")
#df_catalog_history.show(truncate=False)
#df_catalog_history.printSchema()

# ====================================================
# ORDER VALIDATION
# ====================================================


df_orders_parsed = df_orders_raw.withColumn(
    "order_date",
    F.try_to_date("order_date_raw", "yyyy-MM-dd")
).withColumn(
    "updated_at",
    F.try_to_timestamp("updated_at_raw", F.lit("yyyy-MM-dd HH:mm:ss"))
)

df_orders_with_errors = df_orders_parsed.withColumn(
    "validation_errors",
    F.array(
        F.when(F.col("order_date_raw").isNull(), "MISSING_ORDER_DATE"),
        F.when(~F.col("order_date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"), "INVALID_DATE_FORMAT"),
        F.when(F.col("order_date").isNull() & F.col("order_date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"), "INVALID_DATE_VALUE"),
        F.when(F.col("updated_at_raw").isNull(), "MISSING_UPDATED_AT"),
        F.when(F.col("updated_at").isNull() & F.col("updated_at_raw").isNotNull(), "INVALID_TIMESTAMP")
    )
).withColumn(
        "validation_errors",
        F.filter(
            F.col("validation_errors"),
            lambda x: x.isNotNull()
        )
    )

df_orders_valid = df_orders_with_errors.filter(
    F.size(F.col("validation_errors")) == 0
)

df_orders_quarantine = df_orders_with_errors.filter(
    F.size(F.col("validation_errors")) > 0
)

total_rows = df_orders_raw.count()
valid_rows = df_orders_valid.count()
quarantine_rows = df_orders_quarantine.count()

'''
df_orders_quarantine.select(
    "order_id",
    "line_id",
    "line_amount",
    "validation_errors"
).show(truncate=False)'''


source_amount = df_orders_raw.agg(F.sum("line_amount").alias("total_amount")).collect()[0]["total_amount"]
valid_amount = df_orders_valid.agg(F.sum("line_amount").alias("total_amount")).collect()[0]["total_amount"]
quarantine_amount = df_orders_quarantine.agg(F.sum("line_amount").alias("total_amount")).collect()[0]["total_amount"]

assert total_rows == valid_rows + quarantine_rows
assert source_amount == valid_amount + quarantine_amount

# ====================================================
# CATALOG VALIDATION
# ====================================================

df_catalog_parsed = df_catalog_history.withColumn(
    "updated_at",
    F.try_to_timestamp("updated_at_raw", F.lit("yyyy-MM-dd HH:mm:ss"))
)

recency_window = (Window
               .partitionBy("market","product_id")
               .orderBy(F.col("updated_at").desc())
               )

df_catalog_ranked = (
    df_catalog_parsed.withColumn(
        "recency_rank",
        F.dense_rank().over(recency_window)
    )
)
'''
df_catalog_ranked.orderBy(
    "market", "product_id", "recency_rank"
).show(truncate=False)'''

# ====================================================
# DETECT AND CLEAN AMBIGUOUS KEYS
# ====================================================

df_catalog_latest = df_catalog_ranked.filter(
    F.col("recency_rank") == 1
)

df_catalog_ambiguous_keys = (df_catalog_latest
                             .groupBy("market", "product_id")
                             .agg(F.count("*").alias("count"))
                             .filter(F.col("count") > 1)
)

df_catalog_current = df_catalog_latest.join(
    df_catalog_ambiguous_keys,
    on=["market", "product_id"],
    how="left_anti"
)


df_catalog_quarantine = df_catalog_latest.join(
    df_catalog_ambiguous_keys,
    on=["market", "product_id"],
    how="left_semi"
)

print("Latest candidates:", df_catalog_latest.count())
print("Ambiguous keys:", df_catalog_ambiguous_keys.count())
print("Current catalog:", df_catalog_current.count())
print("Quarantine:", df_catalog_quarantine.count())

#df_catalog_current.show(truncate=False)
#df_catalog_quarantine.show(truncate=False)

# ====================================================
# ENRICHMENT
# ====================================================

df_orders_enriched = df_orders_valid.join(
    df_catalog_current[["market", "product_id", "product_name","category"]],
    on=["market", "product_id"],
    how="left"
)

df_ambiguous_flag = (
    df_catalog_ambiguous_keys
    .select("market", "product_id")
    .withColumn("is_catalog_ambiguous", F.lit(True))
)

df_orders_classified = (
    df_orders_enriched.join(
        df_ambiguous_flag,
        on=["market", "product_id"],
        how="left"
    )
    .withColumn(
        "enrichment_status",
        F.when(F.col("is_catalog_ambiguous") == True, F.lit("CATALOG_DUPLICATE"))
         .when(F.col("product_name").isNotNull(), F.lit("ENRICHED"))
         .otherwise(F.lit("CATALOG_MISSING"))
    )
)

'''
print("Valid orders:", df_orders_valid.count())
print("Enriched orders:", df_orders_classified.count())

    
df_orders_classified.groupBy("enrichment_status").agg(
    F.count("*").alias("row_count"),
    F.sum("line_amount").alias("total_amount")
).show() 
'''
# ====================================================
# AGGREGATION AND FINAL RESULTS
# ====================================================

df_market_quality_summary = (
    df_orders_classified
    .groupBy("market", "enrichment_status")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("line_amount").alias("total_amount"),
        F.sum("quantity").alias("total_quantity")
    )
)

df_market_quality_summary.show()

row_count = df_market_quality_summary.agg(F.sum("row_count").alias("total_row_count")).collect()[0]["total_row_count"]
print("Total row count:", row_count)

total_amount = df_market_quality_summary.agg(F.sum("total_amount").alias("total_amount")).collect()[0]["total_amount"]
print("Total amount:", total_amount)

total_quantity = df_market_quality_summary.agg(F.sum("total_quantity").alias("total_quantity")).collect()[0]["total_quantity"]
print("Total quantity:", total_quantity)

assert row_count == df_orders_valid.count(), "Row count mismatch"
assert total_amount == df_orders_valid.agg(F.sum("line_amount").alias("total_amount")).collect()[0]["total_amount"], "Total amount mismatch"
assert total_quantity == df_orders_valid.agg(F.sum("quantity").alias("total_quantity")).collect()[0]["total_quantity"], "Total quantity mismatch"

df_market_quality_summary.explain(mode="formatted")

df_last_check = df_orders_classified.groupBy("order_id","line_id").agg(
    F.count("*").alias("row_count")
)

assert df_last_check.filter(
    F.col("row_count") > 1
).count() == 0, "Duplicate order-line keys detected"

df_missing_lines = df_orders_valid.join(
    df_orders_classified.select("order_id", "line_id"),
    on=["order_id", "line_id"],
    how="left_anti"
)

df_missing_lines.show(truncate=False)

spark.stop()
