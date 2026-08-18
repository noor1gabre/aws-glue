import sys
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# 1. Iceberg static config
conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://crm-datalake-raw/bronze-layer/")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BASE_CHECKPOINT = "s3://crm-datalake-raw/_checkpoints/bronze"

# 2. Read raw stream from Kinesis
kinesis_df = spark.readStream \
    .format("kinesis") \
    .option("streamName", "crm-cdc-stream") \
    .option("region", "eu-north-1") \
    .option("endpointUrl", "https://kinesis.eu-north-1.amazonaws.com") \
    .option("startingPosition", "TRIM_HORIZON") \
    .load()

# 3. Define the DMS envelope schema.
# The "data" field's actual shape differs per table, so we keep it as a
# generic map of string->string here and cast types per-table in the
# per-table transform functions below. This avoids needing 4 different
# readStreams and keeps one single Kinesis consumer (cheaper, simpler).
dms_envelope_schema = StructType([
    StructField("data", StringType(), True),   # kept raw, parsed per-table below
    StructField("metadata", StructType([
        StructField("timestamp", StringType(), True),
        StructField("record-type", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("schema-name", StringType(), True),
        StructField("table-name", StringType(), True),
    ]), True)
])

# Kinesis gives binary "data" column (the whole DMS JSON blob) - cast to string first
raw_df = kinesis_df.selectExpr(
    "CAST(data AS STRING) as json_str",
    "approximateArrivalTimestamp as arrival_time"
)

# We need "data" as its own JSON string (not yet parsed into typed columns,
# since each table has a different shape), plus the metadata fields split out.
parsed_df = raw_df.withColumn(
    "envelope", from_json(col("json_str"), dms_envelope_schema)
).select(
    col("envelope.metadata.table-name").alias("source_table"),
    col("envelope.metadata.record-type").alias("record_type"),
    col("envelope.metadata.operation").alias("dms_operation"),
    col("envelope.metadata.timestamp").alias("dms_timestamp"),
    col("envelope.data").alias("data_json"),   # still a raw JSON string here
    col("arrival_time")
).filter(
    # Drop DMS control records (create-table, etc.) - they have no "data"
    col("record_type") == "data"
).filter(
    # Drop DMS's own internal bookkeeping table
    col("source_table") != "awsdms_apply_exceptions"
)

# 4. Per-table typed schemas for the "data" JSON payload
companies_schema = StructType([
    StructField("company_id", LongType()),
    StructField("name", StringType()),
    StructField("industry", StringType()),
    StructField("website", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

contacts_schema = StructType([
    StructField("contact_id", LongType()),
    StructField("company_id", LongType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

deals_schema = StructType([
    StructField("deal_id", LongType()),
    StructField("company_id", LongType()),
    StructField("contact_id", LongType()),
    StructField("title", StringType()),
    StructField("stage", StringType()),
    StructField("amount", DoubleType()),
    StructField("close_date", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

activities_schema = StructType([
    StructField("activity_id", LongType()),
    StructField("deal_id", LongType()),
    StructField("contact_id", LongType()),
    StructField("type", StringType()),
    StructField("notes", StringType()),
    StructField("activity_date", StringType()),
    StructField("created_at", StringType()),
])

TABLE_CONFIG = {
    "companies": {
        "schema": companies_schema,
        "target": "glue_catalog.crm_bronze_db.bronze_companies",
        "checkpoint": f"{BASE_CHECKPOINT}/companies",
    },
    "contacts": {
        "schema": contacts_schema,
        "target": "glue_catalog.crm_bronze_db.bronze_contacts",
        "checkpoint": f"{BASE_CHECKPOINT}/contacts",
    },
    "deals": {
        "schema": deals_schema,
        "target": "glue_catalog.crm_bronze_db.bronze_deals",
        "checkpoint": f"{BASE_CHECKPOINT}/deals",
    },
    "activities": {
        "schema": activities_schema,
        "target": "glue_catalog.crm_bronze_db.bronze_activities",
        "checkpoint": f"{BASE_CHECKPOINT}/activities",
    },
}


def write_batch(batch_df, batch_id):
    """
    Called once per micro-batch. Splits the batch by source_table and
    writes each slice, with its data JSON parsed into real typed columns,
    into its own Iceberg Bronze table.
    """
    batch_df.persist()

    for table_name, cfg in TABLE_CONFIG.items():
        table_slice = batch_df.filter(col("source_table") == table_name)

        # Parse the per-row JSON "data" payload into typed columns for this table
        typed_slice = table_slice.withColumn(
            "parsed", from_json(col("data_json"), cfg["schema"])
        ).select(
            "parsed.*",
            "dms_operation",
            "dms_timestamp",
            "arrival_time",
        )

        # append() avoids re-triggering a streaming writer per micro-batch;
        # this is a plain batch write against the Iceberg table.
        typed_slice.writeTo(cfg["target"]).append()

    batch_df.unpersist()


# 5. Single streaming query, fanning out to 4 tables per micro-batch via foreachBatch
query = parsed_df.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", f"{BASE_CHECKPOINT}/_main") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()

job.commit()