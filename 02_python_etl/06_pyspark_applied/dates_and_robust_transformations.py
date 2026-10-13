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
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("week6-dates-and-robust-transformations")
    .getOrCreate()
)

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("market", StringType(), False),
    StructField("order_date_raw", StringType(), True),
    StructField("updated_at_raw", StringType(), True),
    StructField("amount", IntegerType(), False),
])

orders_data = [
    ("O1001", "ES", "2026-10-01", "2026-10-01 09:15:00", 120),
    ("O1002", "ES", "2026-10-02", "2026-10-02 18:30:00", 80),
    ("O1003", "PT", "03/10/2026", "2026-10-03 11:10:00", 50),
    ("O1004", "ES", "2026-02-30", "2026-10-04 08:00:00", 75),
    ("O1005", "PT", None, "2026-10-05 14:20:00", 60),
    ("O1006", "ES", "2026-10-06", "not_a_timestamp", 90),
]

df_orders_raw = spark.createDataFrame(
    orders_data,
    schema=orders_schema
)

df_orders_parsed = (
    df_orders_raw
    .withColumn(
        "order_date",
        F.try_to_date("order_date_raw", "yyyy-MM-dd")
    )
    .withColumn(
        "updated_at",
        F.try_to_timestamp("updated_at_raw", F.lit("yyyy-MM-dd HH:mm:ss"))
    )
)


df_orders_validated = (
    df_orders_parsed
    .withColumn(
        "validation_status",
        F.when(
            F.col("order_date_raw").isNull(),
            "MISSING_ORDER_DATE"
        )
        .when(
            ~F.col("order_date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"),
            "INVALID_DATE_FORMAT"
        )
        .when(
            F.col("order_date").isNull(),
            "INVALID_DATE_VALUE"
        )
        .when(
            F.col("updated_at").isNull(),
            "INVALID_TIMESTAMP"
        )
        .otherwise("VALID")
    )
)

df_orders_with_errors = (
    df_orders_validated
    .withColumn(
        "validation_errors",
        F.array(
            F.when(
                F.col("order_date_raw").isNull(),
                F.lit("MISSING_ORDER_DATE")
            ),
            F.when(
                ~F.col("order_date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                F.lit("INVALID_DATE_FORMAT")
            ),
            F.when(
                F.col("order_date").isNull() & F.col("order_date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                F.lit("INVALID_DATE_VALUE")
            ),
            F.when(
                F.col("updated_at_raw").isNull(),
                F.lit("MISSING_UPDATED_AT")
            ),
            F.when(
                F.col("updated_at").isNull() & F.col("updated_at_raw").isNotNull(),
                F.lit("INVALID_TIMESTAMP")
            )
        )
    )
    .withColumn(
        "validation_errors",
        F.filter(
            F.col("validation_errors"),
            lambda x: x.isNotNull()
        )
    )
)



df_orders_with_errors.select(
    "order_id",
    "validation_status",
    "validation_errors"
).show(truncate=False)

df_orders_valid = (
    df_orders_with_errors
    .filter(F.size(F.col("validation_errors")) == 0)
)

df_orders_quarantine = (
    df_orders_with_errors
    .filter(F.size(F.col("validation_errors")) > 0)
)

total_rows = df_orders_with_errors.count()
valid_rows = df_orders_valid.count()
quarantine_rows = df_orders_quarantine.count()

print(f"Total rows: {total_rows}")
print(f"Valid rows: {valid_rows}")
print(f"Quarantine rows: {quarantine_rows}")

df_orders_valid.agg(F.sum("amount").alias("total_amount")).show()
df_orders_quarantine.agg(F.sum("amount").alias("total_amount")).show()

df_orders_quarantine.select(
    "order_id",
    "amount",
    "validation_errors"
).show(truncate=False)