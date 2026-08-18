import sys
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, get_json_object
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# استلام متغيرات الوظيفة
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# 1. إعدادات Iceberg (Static Configs) - يجب تعريفها قبل تشغيل SparkContext
conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# 2. تهيئة الـ Sessions
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 3. قراءة البيانات المتدفقة من Kinesis
kinesis_df = spark.readStream \
    .format("kinesis") \
    .option("streamName", "crm-cdc-stream") \
    .option("region", "eu-north-1") \
    .option("startingPosition", "TRIM_HORIZON") \
    .load()

# 4. تحويل الـ Payload واستخراج الميتاداتا الهامة للـ Partitioning والتحليل اللاحق
bronze_df = kinesis_df.selectExpr(
    "CAST(data AS STRING) as raw_payload",
    "approximateArrivalTimestamp as arrival_time"
).withColumn(
    "source_table", get_json_object(col("raw_payload"), "$.metadata.table-name")
).withColumn(
    "record_type", get_json_object(col("raw_payload"), "$.metadata.record-type")
).withColumn(
    "operation", get_json_object(col("raw_payload"), "$.metadata.operation")
)

# 5. مسار الـ Checkpoint لتتبع تقدم القراءة (تأكد من تعديل اسم الـ Bucket)
checkpoint_path = "s3://crm-datalake-raw/bronze-layer/"

# 6. الكتابة المستمرة في جدول Iceberg مع التقسيم (Partitioning) بناءً على اسم الجدول
query = bronze_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .partitionBy("source_table") \
    .option("checkpointLocation", checkpoint_path) \
    .toTable("glue_catalog.crm_bronze_db.raw_events")

query.awaitTermination()