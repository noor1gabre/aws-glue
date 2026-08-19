import sys
import logging
import time
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
# Glue jobs already stream stdout/stderr to CloudWatch, but plain print()
# gives you no level, timestamp, or module context - hard to filter or
# alarm on. Using the logging module gives structured lines like:
#   2026-08-19 10:02:14 | INFO | silver_merge | [bronze_deals] watermark = ...
# which you can filter by level in CloudWatch Logs Insights.
logger = logging.getLogger("silver_merge")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False  # avoid duplicate lines via the root logger

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

logger.info("Job starting: %s", args['JOB_NAME'])

conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://crm-datalake-raw/")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Spark session and Glue job initialized.")

# ---------------------------------------------------------------------
# Config: one entry per CRM table, describing how to merge Bronze->Silver
# ---------------------------------------------------------------------
MERGE_CONFIG = [
    {
        "bronze_table": "glue_catalog.crm_bronze_db.bronze_companies",
        "silver_table": "glue_catalog.crm_silver_db.silver_companies",
        "pk": "company_id",
        "columns": ["company_id", "name", "industry", "website", "created_at", "updated_at"],
    },
    {
        "bronze_table": "glue_catalog.crm_bronze_db.bronze_contacts",
        "silver_table": "glue_catalog.crm_silver_db.silver_contacts",
        "pk": "contact_id",
        "columns": ["contact_id", "company_id", "first_name", "last_name", "email", "phone", "created_at", "updated_at"],
    },
    {
        "bronze_table": "glue_catalog.crm_bronze_db.bronze_deals",
        "silver_table": "glue_catalog.crm_silver_db.silver_deals",
        "pk": "deal_id",
        "columns": ["deal_id", "company_id", "contact_id", "title", "stage", "amount", "close_date", "created_at", "updated_at"],
    },
    {
        "bronze_table": "glue_catalog.crm_bronze_db.bronze_activities",
        "silver_table": "glue_catalog.crm_silver_db.silver_activities",
        "pk": "activity_id",
        "columns": ["activity_id", "deal_id", "contact_id", "type", "notes", "activity_date", "created_at"],
    },
]

WATERMARK_KEY = "silver_merge.last_arrival_time"


def get_watermark(silver_table):
    """Read the stored watermark from the Iceberg table's properties. None if never run."""
    try:
        props_df = spark.sql(f"SHOW TBLPROPERTIES {silver_table} ('{WATERMARK_KEY}')")
        row = props_df.collect()
        if row and "not set" not in row[0][0]:
            watermark = row[0][1] if len(row[0]) > 1 else row[0][0]
            logger.debug("[%s] found existing watermark: %s", silver_table, watermark)
            return watermark
    except Exception:
        logger.warning(
            "[%s] could not read watermark property, treating as first run.",
            silver_table, exc_info=True,
        )
    logger.info("[%s] no watermark found - this looks like the first run.", silver_table)
    return None


def set_watermark(silver_table, value):
    spark.sql(f"ALTER TABLE {silver_table} SET TBLPROPERTIES ('{WATERMARK_KEY}'='{value}')")
    logger.debug("[%s] watermark property updated to %s", silver_table, value)


def merge_one_table(cfg):
    bronze_table = cfg["bronze_table"]
    silver_table = cfg["silver_table"]
    pk = cfg["pk"]
    columns = cfg["columns"]

    logger.info("=" * 60)
    logger.info("Processing %s -> %s", bronze_table, silver_table)

    start_time = time.time()

    watermark = get_watermark(silver_table)
    logger.info("[%s] using watermark = %s", bronze_table, watermark)

    bronze_df = spark.table(bronze_table)

    if watermark is not None:
        bronze_df = bronze_df.filter(col("arrival_time") > watermark)

    if bronze_df.rdd.isEmpty():
        logger.info("[%s] no new rows since watermark - skipping merge.", bronze_table)
        return

    new_row_count = bronze_df.count()
    logger.info("[%s] found %d new/changed rows since watermark.", bronze_table, new_row_count)

    # Bronze can contain multiple versions of the same PK within this
    # batch window (e.g. a deal changed stage twice). Keep only the
    # latest row per PK, ranked by arrival_time.
    window = Window.partitionBy(pk).orderBy(col("arrival_time").desc())
    latest_per_pk = (
        bronze_df
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    deduped_count = latest_per_pk.count()
    if deduped_count < new_row_count:
        logger.info(
            "[%s] deduplicated %d rows down to %d (multiple changes to same PK in this batch).",
            bronze_table, new_row_count, deduped_count,
        )

    latest_per_pk.createOrReplaceTempView("bronze_latest")

    # Log a quick breakdown of operations in this batch - useful to spot
    # e.g. an unexpected wave of deletes.
    op_counts = latest_per_pk.groupBy("dms_operation").count().collect()
    op_summary = ", ".join(f"{r['dms_operation']}={r['count']}" for r in op_counts)
    logger.info("[%s] operation breakdown: %s", bronze_table, op_summary)

    select_cols = ", ".join(columns)
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in columns if c != pk])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{c}" for c in columns])

    merge_sql = f"""
        MERGE INTO {silver_table} t
        USING (SELECT {select_cols}, dms_operation, arrival_time FROM bronze_latest) s
        ON t.{pk} = s.{pk}
        WHEN MATCHED AND s.dms_operation = 'delete' THEN DELETE
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED AND s.dms_operation != 'delete' THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
    """

    try:
        logger.info("[%s] executing MERGE INTO %s ...", bronze_table, silver_table)
        spark.sql(merge_sql)
    except Exception:
        logger.error(
            "[%s] MERGE INTO %s failed - watermark will NOT be advanced, "
            "so these rows will be retried on the next run.",
            bronze_table, silver_table, exc_info=True,
        )
        raise

    max_arrival = latest_per_pk.agg({"arrival_time": "max"}).collect()[0][0]
    set_watermark(silver_table, str(max_arrival))

    elapsed = time.time() - start_time
    logger.info(
        "[%s] merge complete in %.1fs. new watermark = %s",
        bronze_table, elapsed, max_arrival,
    )


failures = []
job_start = time.time()

for cfg in MERGE_CONFIG:
    try:
        merge_one_table(cfg)
    except Exception:
        # Log and continue to the next table rather than aborting the
        # whole job over one table's failure - but track it so the job
        # ends with a non-zero-visible failure summary.
        logger.error("[%s] table merge failed, continuing with remaining tables.", cfg["bronze_table"])
        failures.append(cfg["bronze_table"])

total_elapsed = time.time() - job_start
logger.info("=" * 60)

if failures:
    logger.error(
        "Job finished with %d failed table(s) out of %d in %.1fs: %s",
        len(failures), len(MERGE_CONFIG), total_elapsed, ", ".join(failures),
    )
else:
    logger.info(
        "Job finished successfully. All %d tables merged in %.1fs.",
        len(MERGE_CONFIG), total_elapsed,
    )

job.commit()

if failures:
    # Non-zero exit so Glue marks the run as FAILED and any alarms/retries fire,
    # even though job.commit() above still ran to preserve bookmark state.
    sys.exit(1)