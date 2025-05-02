import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import upper

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the S3 paths for input and output
input_path = "s3://Input-folder-pyspark/sample_data.csv"
output_path = "s3://target-glue-job-etl/"

# Read from S3 dev input
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Transform - make a column uppercase for example
df = df.withColumn("name_upper", upper(df["name"]))

# Read CSV
df = spark.read.csv(input_path, header=True, inferSchema=True)


# Write to S3 dev output
df.write.mode("overwrite").parquet("s3://your-dev-bucket/output/")

job.commit()




 