import sys
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
# Both bronze and silver tables live in the same Glue Catalog, so one
# catalog config works for both - warehouse path only matters for NEW
# tables created via this catalog, and we already created ours explicitly.
conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://crm-datalake-raw/")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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

# We track, per Silver table, the max arrival_time already merged, stored
# as an Iceberg table property. This makes the job idempotent/incremental:
# each run only processes Bronze rows newer than the last successful merge.
WATERMARK_KEY = "silver_merge.last_arrival_time"


def get_watermark(silver_table):
    """Read the stored watermark from the Iceberg table's properties. None if never run."""
    try:
        props_df = spark.sql(f"SHOW TBLPROPERTIES {silver_table} ('{WATERMARK_KEY}')")
        row = props_df.collect()
        if row and "not set" not in row[0][0]:
            return row[0][1] if len(row[0]) > 1 else row[0][0]
    except Exception:
        pass
    return None


def set_watermark(silver_table, value):
    spark.sql(f"ALTER TABLE {silver_table} SET TBLPROPERTIES ('{WATERMARK_KEY}'='{value}')")


def merge_one_table(cfg):
    bronze_table = cfg["bronze_table"]
    silver_table = cfg["silver_table"]
    pk = cfg["pk"]
    columns = cfg["columns"]

    watermark = get_watermark(silver_table)
    print(f"[{bronze_table}] watermark = {watermark}")

    bronze_df = spark.table(bronze_table)

    if watermark is not None:
        bronze_df = bronze_df.filter(col("arrival_time") > watermark)

    if bronze_df.rdd.isEmpty():
        print(f"[{bronze_table}] no new rows since watermark, skipping.")
        return

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

    latest_per_pk.createOrReplaceTempView("bronze_latest")

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

    print(f"[{bronze_table}] merging into {silver_table} ...")
    spark.sql(merge_sql)

    # Advance the watermark to the max arrival_time we just processed
    max_arrival = latest_per_pk.agg({"arrival_time": "max"}).collect()[0][0]
    set_watermark(silver_table, str(max_arrival))
    print(f"[{bronze_table}] done. new watermark = {max_arrival}")


for cfg in MERGE_CONFIG:
    merge_one_table(cfg)

job.commit()