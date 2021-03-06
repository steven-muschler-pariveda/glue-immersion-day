import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import concat, year, date_format

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://INSERT_BUCKET_NAME/dms-replicate/labdb/customers/"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://INSERT_BUCKET_NAME/dms-replicate/labdb/customers/"], "recurse":True}, transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("operation", "string", "operation", "string"), ("data._id.$oid", "string", "data._id.oid", "string"), ("data.custid", "int", "data.custid", "int"), ("data.trafficfrom", "string", "data.trafficfrom", "string"), ("data.url", "string", "data.url", "string"), ("data.device", "string", "data.device", "string"), ("data.touchproduct", "int", "data.touchproduct", "int"), ("data.trans_timestamp", "string", "data.trans_timestamp", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("operation", "string", "operation", "string"), ("data._id.$oid", "string", "data.oid", "string"), ("data.custid", "int", "data.custid", "int"), ("data.trafficfrom", "string", "data.trafficfrom", "string"), ("data.url", "string", "data.url", "string"), ("data.device", "string", "data.device", "string"), ("data.touchproduct", "int", "data.touchproduct", "int"), ("data.trans_timestamp", "string", "data.trans_timestamp", "timestamp")], transformation_ctx = "Transform0")
## Convert to a pySpark dataframe
DataFrame0 = Transform0.toDF()
## Flatten Struct
DataFrame1 = DataFrame0.select("operation", "data.*")
## Extract year/month
DatesDataFrame0 = DataFrame1.withColumn("year_month", concat(year(DataFrame1.trans_timestamp).cast("string"), date_format(DataFrame1.trans_timestamp, "MM").cast("string")))

DatesDataFrame0.write.option("compression","gzip").partitionBy("year_month").parquet("s3://INSERT_BUCKET_NAME/dms_parquet/customer/",mode="overwrite")

job.commit()
