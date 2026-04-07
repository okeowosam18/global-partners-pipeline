"""
Global Partners - Gold ETL Job
S3 Silver --> S3 Gold Layer

Purpose:
    Calculate all 7 business metrics from the requirements:
    1. Customer Lifetime Value (CLV) — Primary
    2. RFM Segmentation
    3. Churn Indicators
    4. Sales Trends
    5. Loyalty Program Impact
    6. Location Performance
    7. Discount Effectiveness

Upload this file to:
    s3://global-partners-datalake/scripts/gold_job.py
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType, IntegerType, StringType, NullType
from datetime import datetime

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
SILVER_PATH = f"s3://{BUCKET}/silver"
GOLD_PATH   = f"s3://{BUCKET}/gold"

today     = datetime.utcnow()
TODAY_STR = today.strftime("%Y-%m-%d")

print(f"[GOLD] Starting Gold calculations for {TODAY_STR}")

# ============================================================
# Load Silver tables
# ============================================================
print("\n[GOLD] Loading Silver tables...")

df_oi  = spark.read.parquet(f"{SILVER_PATH}/order_items/")
df_oi  = df_oi.toDF(*[c.lower() for c in df_oi.columns])
df_oio = spark.read.parquet(f"{SILVER_PATH}/order_item_options/")
df_oio = df_oio.toDF(*[c.lower() for c in df_oio.columns])
df_dd  = spark.read.parquet(f"{SILVER_PATH}/date_dim/")
df_dd  = df_dd.toDF(*[c.lower() for c in df_dd.columns])

def fix_void_columns(df):
    from pyspark.sql.types import NullType, StringType
    fixed_cols = []
    for field in df.schema.fields:
        if str(field.dataType) == 'NullType' or isinstance(field.dataType, NullType):
            fixed_cols.append(F.col(field.name).cast(StringType()).alias(field.name))
        else:
            fixed_cols.append(F.col(field.name))
    return df.select(fixed_cols)

df_oi  = fix_void_columns(df_oi)
df_oio = fix_void_columns(df_oio)
df_dd  = fix_void_columns(df_dd)

print("order_items schema:")
df_oi.printSchema()
print(f"[GOLD] order_items:        {df_oi.count():,} rows")
print(f"[GOLD] order_item_options: {df_oio.count():,} rows")
print(f"[GOLD] date_dim:           {df_dd.count():,} rows")

# ============================================================
# Build base order-level revenue table
# Join order_items with order_item_options to get full revenue
# ============================================================
print("\n[GOLD] Building base order revenue table...")

# Aggregate option revenue per order/lineitem
df_option_rev = df_oio.groupBy("order_id", "lineitem_id").agg(
    F.sum("option_revenue").alias("total_option_revenue"),
    F.sum(F.when(F.col("is_discount"), F.col("option_revenue")).otherwise(0)).alias("total_discount_amount"),
    F.max(F.col("is_discount").cast("int")).alias("has_discount")
)

# Join with order_items
df_base = df_oi.join(df_option_rev, ["order_id", "lineitem_id"], "left")

# Fill nulls for orders with no options
df_base = df_base \
    .withColumn("total_option_revenue",
        F.coalesce(F.col("total_option_revenue"), F.lit(0))) \
    .withColumn("total_discount_amount",
        F.coalesce(F.col("total_discount_amount"), F.lit(0))) \
    .withColumn("has_discount",
        F.coalesce(F.col("has_discount"), F.lit(0)).cast("boolean"))

# Total revenue per line item
df_base = df_base.withColumn(
    "total_line_revenue",
    F.col("line_revenue") + F.col("total_option_revenue")
)

# Aggregate to order level
df_orders = df_base.groupBy(
    "order_id", "user_id", "restaurant_id",
    "order_date", "order_year", "order_month", "order_hour",
    "is_loyalty", "app_name", "currency"
).agg(
    F.sum("total_line_revenue").alias("order_revenue"),
    F.sum("total_discount_amount").alias("order_discount"),
    F.max("has_discount").alias("has_discount"),
    F.countDistinct("lineitem_id").alias("item_count")
)

# Net revenue after discounts
df_orders = df_orders.withColumn(
    "net_revenue",
    F.col("order_revenue") + F.col("order_discount")  # discount is negative
)

print(f"[GOLD] Base orders table: {df_orders.count():,} orders")


# ============================================================
# METRIC 1: Customer Lifetime Value (CLV) — PRIMARY
# ============================================================
print("\n[GOLD] Calculating CLV...")

# Total CLV per customer
df_clv = df_orders.groupBy("user_id").agg(
    F.sum("net_revenue").alias("total_clv"),
    F.countDistinct("order_id").alias("total_orders"),
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date"),
    F.avg("net_revenue").alias("avg_order_value"),
    F.max("is_loyalty").alias("is_loyalty")
)

# Daily CLV snapshot — how much did each customer spend today
df_daily_clv = df_orders.filter(
    F.col("order_date") == F.lit(TODAY_STR)
).groupBy("user_id", "order_date").agg(
    F.sum("net_revenue").alias("daily_spend"),
    F.countDistinct("order_id").alias("daily_orders")
)

# CLV percentile tiers — High (top 20%), Medium (mid 60%), Low (bottom 20%)
clv_percentiles = df_clv.approxQuantile("total_clv", [0.20, 0.80], 0.01)
low_threshold   = clv_percentiles[0]
high_threshold  = clv_percentiles[1]

df_clv = df_clv.withColumn(
    "clv_tier",
    F.when(F.col("total_clv") >= high_threshold, "High")
     .when(F.col("total_clv") >= low_threshold,  "Medium")
     .otherwise("Low")
)

# Add ingestion date
df_clv = df_clv.withColumn("snapshot_date", F.lit(TODAY_STR))

df_clv.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_daily_clv/")
print(f"[GOLD] CLV written: {df_clv.count():,} customers")


# ============================================================
# METRIC 2: Customer Dimension + RFM Segmentation
# ============================================================
print("\n[GOLD] Calculating RFM segmentation...")

# Reference date for recency calculation
ref_date = F.lit(TODAY_STR).cast("date")

# RFM metrics per customer
df_rfm = df_orders.groupBy("user_id").agg(
    F.datediff(ref_date, F.max("order_date")).alias("recency_days"),
    F.countDistinct("order_id").alias("frequency"),
    F.sum("net_revenue").alias("monetary")
)

# Score each dimension 1-3 (3 = best)
recency_p  = df_rfm.approxQuantile("recency_days", [0.33, 0.67], 0.01)
frequency_p = df_rfm.approxQuantile("frequency",   [0.33, 0.67], 0.01)
monetary_p  = df_rfm.approxQuantile("monetary",    [0.33, 0.67], 0.01)

df_rfm = df_rfm \
    .withColumn("r_score",
        F.when(F.col("recency_days") <= recency_p[0], 3)
         .when(F.col("recency_days") <= recency_p[1], 2)
         .otherwise(1)) \
    .withColumn("f_score",
        F.when(F.col("frequency") >= frequency_p[1], 3)
         .when(F.col("frequency") >= frequency_p[0], 2)
         .otherwise(1)) \
    .withColumn("m_score",
        F.when(F.col("monetary") >= monetary_p[1], 3)
         .when(F.col("monetary") >= monetary_p[0], 2)
         .otherwise(1))

df_rfm = df_rfm.withColumn(
    "rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score")
)

# Segment labels
df_rfm = df_rfm.withColumn(
    "segment",
    F.when((F.col("r_score") == 3) & (F.col("f_score") == 3) & (F.col("m_score") == 3), "VIP")
     .when((F.col("r_score") == 3) & (F.col("f_score") == 1), "New Customer")
     .when((F.col("r_score") == 1) & (F.col("f_score") == 1), "Churn Risk")
     .when(F.col("rfm_score") >= 7, "Loyal")
     .when(F.col("rfm_score") >= 5, "Potential")
     .otherwise("At Risk")
)

df_rfm = df_rfm.withColumn("snapshot_date", F.lit(TODAY_STR))

df_rfm.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_rfm_segments/")
print(f"[GOLD] RFM written: {df_rfm.count():,} customers")


# ============================================================
# METRIC 3: Churn Indicators
# ============================================================
print("\n[GOLD] Calculating churn indicators...")

df_churn = df_orders.groupBy("user_id").agg(
    F.datediff(ref_date, F.max("order_date")).alias("days_since_last_order"),
    F.countDistinct("order_id").alias("total_orders"),
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date"),
    F.sum("net_revenue").alias("total_spend")
)

# Average gap between orders (tenure days / orders)
df_churn = df_churn.withColumn(
    "tenure_days",
    F.datediff(F.col("last_order_date"), F.col("first_order_date"))
)
df_churn = df_churn.withColumn(
    "avg_order_gap_days",
    F.when(F.col("total_orders") > 1,
        F.col("tenure_days") / (F.col("total_orders") - 1)
    ).otherwise(None)
)

# Spend trend — last 30 days vs prior 30 days
df_recent = df_orders.filter(
    F.datediff(ref_date, F.col("order_date")) <= 30
).groupBy("user_id").agg(
    F.sum("net_revenue").alias("spend_last_30d")
)

df_prior = df_orders.filter(
    (F.datediff(ref_date, F.col("order_date")) > 30) &
    (F.datediff(ref_date, F.col("order_date")) <= 60)
).groupBy("user_id").agg(
    F.sum("net_revenue").alias("spend_prior_30d")
)

df_churn = df_churn \
    .join(df_recent, "user_id", "left") \
    .join(df_prior,  "user_id", "left")

df_churn = df_churn.withColumn(
    "spend_pct_change",
    F.when(
        (F.col("spend_prior_30d").isNotNull()) & (F.col("spend_prior_30d") != 0),
        ((F.col("spend_last_30d") - F.col("spend_prior_30d")) / F.col("spend_prior_30d") * 100)
    ).otherwise(None)
)

# Churn risk tag
df_churn = df_churn.withColumn(
    "churn_risk_tag",
    F.when(F.col("days_since_last_order") > 45, "At Risk")
     .when(F.col("days_since_last_order") > 30, "Warning")
     .otherwise("Active")
)

df_churn = df_churn.withColumn("snapshot_date", F.lit(TODAY_STR))

df_churn.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_churn_indicators/")
print(f"[GOLD] Churn written: {df_churn.count():,} customers")


# ============================================================
# METRIC 4: Sales Trends
# ============================================================
print("\n[GOLD] Calculating sales trends...")

# Add item_category if it exists, otherwise create empty string column
if "item_category" in df_oi.columns:
    df_orders_trends = df_orders.join(
        df_oi.select("order_id", "lineitem_id", "item_category").distinct(),
        "order_id", "left"
    )
else:
    df_orders_trends = df_orders.withColumn("item_category", F.lit("Unknown").cast(StringType()))

df_daily = df_orders_trends.groupBy("order_date", "restaurant_id", "item_category").agg(
    F.sum("net_revenue").alias("daily_revenue"),
    F.countDistinct("order_id").alias("daily_orders"),
    F.countDistinct("user_id").alias("unique_customers"),
    F.avg("net_revenue").alias("avg_order_value")
)

# Join with date_dim for calendar context
df_daily = df_daily.join(
    df_dd.select("date_key", "day_of_week", "week", "month", "year", "is_weekend", "is_holiday", "holiday_name"),
    df_daily["order_date"] == df_dd["date_key"],
    "left"
).drop("date_key")

df_daily.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_sales_trends/")
print(f"[GOLD] Sales trends written: {df_daily.count():,} rows")

# ============================================================
# METRIC 5: Loyalty Program Impact
# ============================================================
print("\n[GOLD] Calculating loyalty impact...")

df_loyalty = df_orders.groupBy("user_id", "is_loyalty").agg(
    F.sum("net_revenue").alias("total_spend"),
    F.countDistinct("order_id").alias("total_orders"),
    F.avg("net_revenue").alias("avg_order_value"),
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date")
)

# CLV join for loyalty comparison
df_loyalty = df_loyalty.join(
    df_clv.select("user_id", "clv_tier"),
    "user_id", "left"
)

# Summary by loyalty status
df_loyalty_summary = df_loyalty.groupBy("is_loyalty").agg(
    F.count("user_id").alias("customer_count"),
    F.avg("total_spend").alias("avg_clv"),
    F.avg("total_orders").alias("avg_orders"),
    F.avg("avg_order_value").alias("avg_order_value"),
    F.sum("total_spend").alias("total_revenue")
)

df_loyalty_summary = df_loyalty_summary.withColumn("snapshot_date", F.lit(TODAY_STR))

df_loyalty_summary.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_loyalty_comparison/")
print(f"[GOLD] Loyalty written: {df_loyalty_summary.count():,} rows")


# ============================================================
# METRIC 6: Location Performance
# ============================================================
print("\n[GOLD] Calculating location performance...")

df_location = df_orders.groupBy("restaurant_id").agg(
    F.sum("net_revenue").alias("total_revenue"),
    F.avg("net_revenue").alias("avg_order_value"),
    F.countDistinct("order_id").alias("total_orders"),
    F.countDistinct("user_id").alias("unique_customers"),
    F.countDistinct("order_date").alias("active_days")
)

# Orders per day
df_location = df_location.withColumn(
    "orders_per_day",
    F.col("total_orders") / F.col("active_days")
)

# Revenue rank
window_rank = Window.orderBy(F.desc("total_revenue"))
df_location = df_location.withColumn("revenue_rank", F.rank().over(window_rank))

df_location = df_location.withColumn(
    "performance_tier",
    F.when(F.col("revenue_rank") <= 5, "Top Performer")
     .when(F.col("revenue_rank") <= 10, "Mid Performer")
     .otherwise("Low Performer")
)

df_location = df_location.withColumn("snapshot_date", F.lit(TODAY_STR))

df_location.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_location_performance/")
print(f"[GOLD] Location performance written: {df_location.count():,} locations")


# ============================================================
# METRIC 7: Discount Effectiveness
# ============================================================
print("\n[GOLD] Calculating discount effectiveness...")

df_discount = df_orders.groupBy("has_discount", "order_date").agg(
    F.countDistinct("order_id").alias("order_count"),
    F.sum("order_revenue").alias("gross_revenue"),
    F.sum("order_discount").alias("total_discount_amount"),
    F.sum("net_revenue").alias("net_revenue"),
    F.avg("net_revenue").alias("avg_order_value"),
    F.countDistinct("user_id").alias("unique_customers")
)

# Discount rate
df_discount = df_discount.withColumn(
    "discount_rate_pct",
    F.when(
        F.col("gross_revenue") != 0,
        F.abs(F.col("total_discount_amount")) / F.col("gross_revenue") * 100
    ).otherwise(0)
)

df_discount = df_discount.withColumn("snapshot_date", F.lit(TODAY_STR))

df_discount.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_discount_effectiveness/")
print(f"[GOLD] Discount effectiveness written: {df_discount.count():,} rows")


# ============================================================
# Customer Dimension Table
# ============================================================
print("\n[GOLD] Building customer dimension...")

df_dim_customer = df_clv.join(
    df_rfm.select("user_id", "recency_days", "frequency", "monetary", "rfm_score", "segment"),
    "user_id", "left"
).join(
    df_churn.select("user_id", "days_since_last_order", "churn_risk_tag"),
    "user_id", "left"
)

df_dim_customer.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_customer/")
print(f"[GOLD] Customer dimension written: {df_dim_customer.count():,} customers")

print("\n[GOLD] All Gold metrics calculated successfully.")
job.commit()
