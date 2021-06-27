import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat, year, date_format, row_number, desc
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://jd-immersion-day-test-dincher/dms-replicate/labdb/customers/"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
Updates = glueContext.create_dynamic_frame.from_options(format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://jd-immersion-day-test-dincher/dms_updates/"], "recurse":True}, transformation_ctx = "Updates")

Records = spark.read.load("s3a://jd-immersion-day-test-dincher/dms_parquet/")
#= glueContext.create_dynamic_frame.from_options(format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "parquet", connection_options = {"paths": ["s3://jd-immersion-day-test-dincher/dms_parquet/"], "recurse":True}, transformation_ctx = "Records").toDF()
## @type: ApplyMapping
## @args: [mappings = [("operation", "string", "operation", "string"), ("data._id.$oid", "string", "data._id.oid", "string"), ("data.custid", "int", "data.custid", "int"), ("data.trafficfrom", "string", "data.trafficfrom", "string"), ("data.url", "string", "data.url", "string"), ("data.device", "string", "data.device", "string"), ("data.touchproduct", "int", "data.touchproduct", "int"), ("data.trans_timestamp", "string", "data.trans_timestamp", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform = ApplyMapping.apply(frame = Updates, mappings = [("operation", "string", "operation", "string"), ("data._id.$oid", "string", "data.oid", "string"), ("data.custid", "int", "data.custid", "int"), ("data.trafficfrom", "string", "data.trafficfrom", "string"), ("data.url", "string", "data.url", "string"), ("data.device", "string", "data.device", "string"), ("data.touchproduct", "int", "data.touchproduct", "int"), ("data.trans_timestamp", "string", "data.trans_timestamp", "timestamp")], transformation_ctx = "Transform")

UpdatesDataFrame = Transform.toDF().select("operation", "data.*")

UpdatesDataFrame = UpdatesDataFrame.withColumn("year_month", concat(year(UpdatesDataFrame.trans_timestamp).cast("string"), date_format(UpdatesDataFrame.trans_timestamp, "MM").cast("string")))

MergedDataFrame = UpdatesDataFrame.union(Records)

MergedDataFrame = MergedDataFrame.withColumn("_row_number", row_number().over(Window.partitionBy (MergedDataFrame['oid']).orderBy(desc('trans_timestamp'))))

MergedDataFrame = MergedDataFrame.where(MergedDataFrame._row_number == 1).drop("_row_number")

MergedDataFrame.write.option("compression","gzip").partitionBy("year_month").parquet("s3://jd-immersion-day-test-dincher/dms_temp/customer/",mode="overwrite")

Output = spark.read.load("s3a://jd-immersion-day-test-dincher/dms_temp/")

Output.write.option("compression","gzip").partitionBy("year_month").parquet("s3://jd-immersion-day-test-dincher/dms_parquet/customer/",mode="overwrite")

job.commit()
