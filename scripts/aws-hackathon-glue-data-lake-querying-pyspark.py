# importing modules
from awsglue.job import Job
import sys, os, boto3, logging
from awsglue.transforms import *
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")
sns_client = boto3.client('sns')
glue_client = boto3.client('glue')

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'csv_file', 'parquet_file', 'file', 'query', 'table_name', 'sns_topic_arn', 'crawler_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket_name = args['bucket_name']
csv_file_path = args['csv_file']
parquet_file_path = args['parquet_file']
input_file_format = args['file']
query = args['query']
table_name = args['table_name']
sns_topic_arn = args['sns_topic_arn']
output_path = f"s3://{bucket_name}/output/"

# Spark optimizations
spark.conf.set("fs.s3.maxConnections", "100")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

def send_email_notification(subject, message):
    """Send an SNS email notification."""
    try:
        sns_client.publish(TopicArn=sns_topic_arn, Message=message, Subject=subject)
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {e}")

def delete_existing_files():
    """Delete existing output files."""
    try:
        s3_client.delete_object(Bucket=bucket_name, Key='output/result.csv')
        s3_client.delete_object(Bucket=bucket_name, Key='output/result.parquet')
    except Exception as e:
        logger.warning(f"Failed to delete existing files: {e}")

def read_input_file():
    """Read input file based on the format."""
    try:
        path = f"s3://{bucket_name}/input_data/{input_file_format}/"
        if input_file_format == "csv":
            logger.info("Reading CSV file as input.")
            return glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": [path + csv_file_path]},
                format="csv",
                format_options={"withHeader": True, "separator": ","},
            )
        elif input_file_format == "parquet":
            logger.info("Reading Parquet file as input.")
            return glueContext.create_dynamic_frame.from_options(
                connection_type="s3", connection_options={"paths": [path + parquet_file_path]}, format="parquet"
            )
        else:
            send_email_notification(
                "Execution Failed",
                f"Unsupported file format: {input_file_format}. Supported formats are 'csv' and 'parquet'. Exiting.",
            )
            sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        sys.exit(1)

dynamic_frame = read_input_file()
df = dynamic_frame.toDF()

# Data Validation
logger.info(f"Total Rows: {df.count()}")
logger.info(f"Total Columns: {len(df.columns)}")
logger.info(f"Columns: {df.columns}")

duplicate_count = df.count() - df.distinct().count()
print(f"Number of duplicate rows: {duplicate_count}")

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
logger.info("Null counts:")
null_counts.show()

duplicate_count = df.count() - df.distinct().count()
logger.info(f"Number of duplicate rows: {duplicate_count}")

df = df.fillna({"Phone": "Unknown"})

# Data Cleaning
df = df.dropna().dropDuplicates()
df.createOrReplaceTempView(table_name)

# Transformations
logger.info("Applying transformations...")
df = df.withColumn("age", (datediff(current_date(), to_date(col("Date of Birth"), "yyyy-MM-dd")) / 365).cast("int"))
df = df.filter(col("age") > 30)

# Query Execution
logger.info(f"Executing query: {query}")
result_df = spark.sql(query)

# Delete Old Files and Write New Data
delete_existing_files()

output_format = "parquet" if input_file_format == "csv" else "csv"
logger.info(f"Writing output data as {output_format.upper()} format.")
if output_format == "parquet":
    result_df.write.option("compression", "snappy").mode("overwrite").parquet(output_path)
else:
    result_df.write.option("header", "true").mode("overwrite").csv(output_path)

# Start Crawler
try:
    glue_client.start_crawler(Name=args["crawler_name"])
except glue_client.exceptions.CrawlerRunningException:
    logger.info(f"Crawler '{args['crawler_name']}' is already running.")
except Exception as e:
    logger.error(f"Failed to start crawler: {e}")
    send_email_notification("Execution Failed", f"Error starting crawler: {e}")

# Final Notification
send_email_notification(
    "Execution Completed",
    "Dear Team,\nThe Glue job executed successfully. Check the results in Athena.\n\nThanks!",
)
logger.info("Execution Completed.")

job.commit()