import sys
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ---------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------
logger = logging.getLogger("silver_merge")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

# ---------------------------------------------------------------------
# Argument Parsing (Handling Lambda Triggers & Manual Runs)
# ---------------------------------------------------------------------
args_keys = ['JOB_NAME']
# Safely check if Lambda passed the specific tables to process
if '--tables_to_process' in sys.argv:
    args_keys.append('tables_to_process')
args = getResolvedOptions(sys.argv, args_keys)

# Parse the target tables. If empty (e.g., manual run), it falls back to an empty list.
tables_to_process_str = args.get('tables_to_process', '')
tables_to_process = tables_to_process_str.split(',') if tables_to_process_str else []

logger.info("Job starting: %s", args['JOB_NAME'])
logger.info("Tables triggered by Lambda: %s", tables_to_process)

# ---------------------------------------------------------------------
# Spark & Iceberg Configuration
# ---------------------------------------------------------------------
conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------------------------------------------------------------------
# Merge Configurations
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

# ---------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------
def get_watermark(silver_table):
    """
    Reads the stored watermark from the Iceberg table properties.
    Returns None if the property is missing or the table has never been processed.
    """
    try:
        props_df = spark.sql(f"SHOW TBLPROPERTIES {silver_table} ('{WATERMARK_KEY}')")
        row = props_df.collect()
        
        if not row:
            return None
            
        value = str(row[0][1]) if len(row[0]) > 1 else str(row[0][0])
        
        # Bypass the string error returned by Spark 3 when the property is missing
        if "does not have property" in value or "not set" in value:
            return None
            
        return value
    except Exception:
        return None

def set_watermark(silver_table, value):
    """Updates the watermark property in the Iceberg table."""
    spark.sql(f"ALTER TABLE {silver_table} SET TBLPROPERTIES ('{WATERMARK_KEY}'='{value}')")

def merge_one_table(cfg):
    """
    Handles the end-to-end incremental merge process for a single table.
    """
    bronze_table = cfg["bronze_table"]
    silver_table = cfg["silver_table"]
    pk = cfg["pk"]
    columns = cfg["columns"]

    logger.info("Processing %s -> %s", bronze_table, silver_table)
    start_time = time.time()

    watermark = get_watermark(silver_table)
    bronze_df = spark.table(bronze_table)

    # Filter for new records based on the watermark
    if watermark is not None:
        bronze_df = bronze_df.filter(col("arrival_time") > watermark)

    # Fast empty check: limit(1).count() avoids the severe performance penalty of rdd.isEmpty()
    if bronze_df.limit(1).count() == 0:
        logger.info("[%s] No new rows since watermark - skipping.", bronze_table)
        return

    # Deduplication: Rank and select the latest record for each Primary Key within this batch
    window = Window.partitionBy(pk).orderBy(col("arrival_time").desc())
    latest_per_pk = (
        bronze_df
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    
    # Cache the DataFrame to improve performance during the MERGE and MAX(time) calculations
    latest_per_pk.cache()
    latest_per_pk.createOrReplaceTempView(f"bronze_latest_{pk}")

    # Build dynamic MERGE components
    select_cols = ", ".join(columns)
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in columns if c != pk])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{c}" for c in columns])

    merge_sql = f"""
        MERGE INTO {silver_table} t
        USING (SELECT {select_cols}, dms_operation, arrival_time FROM bronze_latest_{pk}) s
        ON t.{pk} = s.{pk}
        WHEN MATCHED AND s.dms_operation = 'delete' THEN DELETE
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED AND s.dms_operation != 'delete' THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
    """

    try:
        spark.sql(merge_sql)
    except Exception as e:
        logger.error("[%s] MERGE INTO failed: %s", bronze_table, str(e))
        raise

    # Advance the watermark to the max arrival_time processed in this batch
    max_arrival = latest_per_pk.agg({"arrival_time": "max"}).collect()[0][0]
    set_watermark(silver_table, str(max_arrival))

    latest_per_pk.unpersist()
    elapsed = time.time() - start_time
    logger.info("[%s] Merge complete in %.1fs. New watermark = %s", bronze_table, elapsed, max_arrival)

# ---------------------------------------------------------------------
# Execution Logic (Parallel Processing)
# ---------------------------------------------------------------------
job_start = time.time()
failures = []

# Filter configurations based on Lambda arguments
active_configs = []
for cfg in MERGE_CONFIG:
    table_short_name = cfg["bronze_table"].split('.')[-1]
    
    # If the list is empty (manual run), process all tables. 
    # Otherwise, process only the target tables passed by Lambda.
    if not tables_to_process or table_short_name in tables_to_process:
        active_configs.append(cfg)

logger.info("Executing merges in parallel for %d tables...", len(active_configs))

# Execute the merge operations in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=len(active_configs) if active_configs else 1) as executor:
    future_to_table = {executor.submit(merge_one_table, cfg): cfg["bronze_table"] for cfg in active_configs}
    
    for future in as_completed(future_to_table):
        table_name = future_to_table[future]
        try:
            future.result()
        except Exception as exc:
            logger.error("[%s] Table merge failed and threw an exception.", table_name)
            failures.append(table_name)

total_elapsed = time.time() - job_start
logger.info("=" * 60)

if failures:
    logger.error("Job finished with %d failed table(s) in %.1fs: %s", len(failures), total_elapsed, ", ".join(failures))
else:
    logger.info("Job finished successfully. All active tables merged in %.1fs.", total_elapsed)

job.commit()

if failures:
    # Exit with a non-zero status so Glue properly registers the run as FAILED
    sys.exit(1)