"""
Global Partners - Silver ETL Job
S3 Bronze --> S3 Silver Layer

Purpose:
    Clean, validate, deduplicate, and cast data types.
    Silver is the single source of truth for all Gold calculations.

Upload this file to:
    s3://global-partners-datalake/scripts/silver_job.py
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, DecimalType,
    TimestampType, DateType, BooleanType, NullType
)
from datetime import datetime


def fix_void_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, F.col(field.name).cast(StringType()))
    return df
# ============================================================
# Initialize Glue Context
# ============================================================
args        = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ============================================================
# Configuration
# ============================================================
BUCKET      = "global-partners-datalake"
BRONZE_PATH = f"s3://{BUCKET}/bronze"
SILVER_PATH = f"s3://{BUCKET}/silver"

today = datetime.utcnow()
YEAR  = str(today.year)
MONTH = str(today.month).zfill(2)
DAY   = str(today.day).zfill(2)

print(f"[SILVER] Starting Silver transform for {YEAR}-{MONTH}-{DAY}")

# ============================================================
# Helper: log data quality stats
# ============================================================
def log_quality(df, table_name, stage):
    count = df.count()
    print(f"[SILVER] {table_name} | {stage}: {count:,} rows")
    return count


# ============================================================
# 1. ORDER ITEMS — Clean & Cast
# ============================================================
print("\n[SILVER] Processing order_items...")

df_oi = spark.read.parquet(f"{BRONZE_PATH}/order_items/")
df_oi = df_oi.toDF(*[c.lower() for c in df_oi.columns])
log_quality(df_oi, "order_items", "raw bronze")

# Cast columns to correct types
df_oi = df_oi \
    .withColumn("restaurant_id",       F.col("restaurant_id").cast(StringType())) \
    .withColumn("creation_time_utc",   F.to_timestamp(F.col("creation_time_utc"))) \
    .withColumn("order_id",            F.col("order_id").cast(StringType())) \
    .withColumn("user_id",             F.col("user_id").cast(StringType())) \
    .withColumn("printed_card_number", F.col("printed_card_number").cast(StringType())) \
    .withColumn("is_loyalty",          F.col("is_loyalty").cast(BooleanType())) \
    .withColumn("currency",            F.col("currency").cast(StringType())) \
    .withColumn("lineitem_id",         F.col("lineitem_id").cast(StringType())) \
    .withColumn("item_category",       F.col("item_category").cast(StringType())) \
    .withColumn("item_name",           F.col("item_name").cast(StringType())) \
    .withColumn("item_price",          F.col("item_price").cast(DecimalType(10, 2))) \
    .withColumn("item_quantity",       F.col("item_quantity").cast(IntegerType()))

# Clean item_category — strip URLs (dirty data found during Bronze load)
df_oi = df_oi.withColumn(
    "item_category",
    F.regexp_replace(F.col("item_category"), r"https?://\S+", "")
)
df_oi = df_oi.withColumn(
    "item_category",
    F.trim(F.col("item_category"))
)

# Replace empty strings with null
for col_name in ["item_category", "item_name", "app_name", "currency", "printed_card_number"]:
    df_oi = df_oi.withColumn(
        col_name,
        F.when(F.col(col_name) == "", None).otherwise(F.col(col_name))
    )

# Drop rows missing critical identifiers
before = df_oi.count()
df_oi = df_oi.filter(
    F.col("order_id").isNotNull() &
    F.col("user_id").isNotNull() &
    F.col("lineitem_id").isNotNull()
)
after = df_oi.count()
print(f"[SILVER] order_items: dropped {before - after:,} rows with null key columns")

# Deduplicate on natural key
df_oi = df_oi.dropDuplicates(["order_id", "lineitem_id"])
log_quality(df_oi, "order_items", "after dedup")

# Add derived date columns for easier aggregation
df_oi = df_oi \
    .withColumn("order_date",  F.to_date(F.col("creation_time_utc"))) \
    .withColumn("order_year",  F.year(F.col("creation_time_utc"))) \
    .withColumn("order_month", F.month(F.col("creation_time_utc"))) \
    .withColumn("order_hour",  F.hour(F.col("creation_time_utc")))

# Compute line item revenue (price * quantity)
df_oi = df_oi.withColumn(
    "line_revenue",
    F.col("item_price") * F.col("item_quantity")
)

# Write Silver order_items
df_oi  = fix_void_columns(df_oi)
df_oi.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(f"{SILVER_PATH}/order_items/")

print(f"[SILVER] order_items written to Silver")


# ============================================================
# 2. ORDER ITEM OPTIONS — Clean & Cast
# ============================================================
print("\n[SILVER] Processing order_item_options...")

df_oio = spark.read.parquet(f"{BRONZE_PATH}/order_item_options/")
df_oio = df_oio.toDF(*[c.lower() for c in df_oio.columns])
log_quality(df_oio, "order_item_options", "raw bronze")

df_oio = df_oio \
    .withColumn("order_id",         F.col("order_id").cast(StringType())) \
    .withColumn("lineitem_id",      F.col("lineitem_id").cast(StringType())) \
    .withColumn("option_group_name",F.col("option_group_name").cast(StringType())) \
    .withColumn("option_name",      F.col("option_name").cast(StringType())) \
    .withColumn("option_price",     F.col("option_price").cast(DecimalType(10, 2))) \
    .withColumn("option_quantity",  F.col("option_quantity").cast(IntegerType()))

# Replace empty strings with null
for col_name in ["option_group_name", "option_name"]:
    df_oio = df_oio.withColumn(
        col_name,
        F.when(F.col(col_name) == "", None).otherwise(F.col(col_name))
    )

# Drop rows missing critical identifiers
before = df_oio.count()
df_oio = df_oio.filter(
    F.col("order_id").isNotNull() &
    F.col("lineitem_id").isNotNull()
)
after = df_oio.count()
print(f"[SILVER] order_item_options: dropped {before - after:,} rows with null keys")

# Deduplicate
df_oio = df_oio.dropDuplicates(["order_id", "lineitem_id", "option_name"])

# Flag discounts (negative option_price)
df_oio = df_oio.withColumn(
    "is_discount",
    F.when(F.col("option_price") < 0, True).otherwise(False)
)

# Compute option revenue contribution
df_oio = df_oio.withColumn(
    "option_revenue",
    F.col("option_price") * F.col("option_quantity")
)

log_quality(df_oio, "order_item_options", "after dedup")

df_oio = fix_void_columns(df_oio)
df_oio.write \
    .mode("overwrite") \
    .parquet(f"{SILVER_PATH}/order_item_options/")

print(f"[SILVER] order_item_options written to Silver")


# ============================================================
# 3. DATE DIM — Clean & Cast
# ============================================================
print("\n[SILVER] Processing date_dim...")

df_dd = spark.read.parquet(f"{BRONZE_PATH}/date_dim/")
df_dd = df_dd.toDF(*[c.lower() for c in df_dd.columns])
log_quality(df_dd, "date_dim", "raw bronze")

df_dd = df_dd \
    .withColumn("date_key",    F.to_date(F.col("date_key"))) \
    .withColumn("day_of_week", F.col("day_of_week").cast(StringType())) \
    .withColumn("week",        F.col("week").cast(IntegerType())) \
    .withColumn("month",       F.col("month").cast(StringType())) \
    .withColumn("year",        F.col("year").cast(IntegerType())) \
    .withColumn("is_weekend",  F.col("is_weekend").cast(BooleanType())) \
    .withColumn("is_holiday",  F.col("is_holiday").cast(BooleanType())) \
    .withColumn("holiday_name",F.col("holiday_name").cast(StringType()))

df_dd = df_dd.dropDuplicates(["date_key"])
log_quality(df_dd, "date_dim", "after dedup")

df_dd  = fix_void_columns(df_dd)
df_dd.write \
    .mode("overwrite") \
    .parquet(f"{SILVER_PATH}/date_dim/")

print(f"[SILVER] date_dim written to Silver")

print("\n[SILVER] All tables processed successfully.")
job.commit()
