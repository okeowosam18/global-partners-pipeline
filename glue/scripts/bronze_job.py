"""
Global Partners - Bronze ETL Job
SQL Server (via JDBC) --> S3 Bronze Layer

Purpose:
    Raw ingestion only. No transformations. Data lands exactly
    as it exists in SQL Server, partitioned by ingestion date.

Upload this file to:
    s3://global-partners-datalake/scripts/bronze_job.py

Then create a Glue Job pointing to that S3 path.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_date, year, month, dayofmonth, lit
import boto3
from datetime import datetime

# ============================================================
# Initialize Glue Context
# ============================================================
args    = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc      = SparkContext()
glueContext = GlueContext(sc)
spark   = glueContext.spark_session
job     = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ============================================================
# Configuration
# ============================================================
BUCKET          = "global-partners-datalake"
BRONZE_PATH     = f"s3://{BUCKET}/bronze"
GLUE_DB         = "global_partners_raw"
CONNECTION_NAME = "global-partners-sqlserver"

# Tables to ingest from SQL Server
TABLES = [
    "order_items",
    "order_item_options",
    "date_dim"
]

# Today's partition values
today     = datetime.utcnow()
YEAR      = str(today.year)
MONTH     = str(today.month).zfill(2)
DAY       = str(today.day).zfill(2)

print(f"[BRONZE] Starting ingestion for {YEAR}-{MONTH}-{DAY}")

# ============================================================
# Ingest each table from SQL Server via JDBC
# ============================================================
for table_name in TABLES:
    print(f"[BRONZE] Ingesting table: {table_name}")

    try:
        # Read from Glue Data Catalog (which points to SQL Server via JDBC)
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database   = GLUE_DB,
        table_name = f"globalpartners_dbo_{table_name}"
        )

        # Convert to Spark DataFrame
        df = dynamic_frame.toDF()

        # Add ingestion metadata columns
        df = df.withColumn("_ingestion_date",  lit(f"{YEAR}-{MONTH}-{DAY}"))
        df = df.withColumn("_ingestion_year",  lit(YEAR))
        df = df.withColumn("_ingestion_month", lit(MONTH))
        df = df.withColumn("_ingestion_day",   lit(DAY))

        row_count = df.count()
        print(f"[BRONZE] {table_name}: {row_count:,} rows read from SQL Server")

        # Write to S3 Bronze as Parquet, partitioned by date
        output_path = f"{BRONZE_PATH}/{table_name}"

        df.write \
            .mode("overwrite") \
            .partitionBy("_ingestion_year", "_ingestion_month", "_ingestion_day") \
            .parquet(output_path)

        print(f"[BRONZE] {table_name}: written to {output_path}")

    except Exception as e:
        print(f"[BRONZE] ERROR on {table_name}: {str(e)}")
        raise e

print("[BRONZE] All tables ingested successfully.")
job.commit()