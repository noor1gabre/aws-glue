import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. إعداد الـ Spark Session لدعم Iceberg
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# 2. قراءة البيانات المتدفقة من Kinesis
kinesis_df = spark.readStream \
    .format("kinesis") \
    .option("streamName", "crm-cdc-stream") \
    .option("region", "eu-north-1") \
    .option("startingPosition", "TRIM_HORIZON") \
    .load()

# 3. تحويل الـ Payload من Binary إلى String (JSON)
# سنحتفظ بكل بيانات الرسالة في عمود واحد كـ Raw Data لحفظها في الـ Bronze
bronze_df = kinesis_df.selectExpr(
    "CAST(data AS STRING) as raw_payload",
    "approximateArrivalTimestamp as arrival_time",
    "partitionKey as kinesis_partition_key"
)

# 4. تحديد مسار الـ Checkpoint في S3 (لتتبع تقدم القراءة)
checkpoint_path = "s3://crm-datalake-raw/bronze-layer/"

# 5. الكتابة المستمرة (Streaming) في جدول Iceberg في طبقة الـ Bronze
query = bronze_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .toTable("glue_catalog.crm_bronze_db.raw_events")

query.awaitTermination()