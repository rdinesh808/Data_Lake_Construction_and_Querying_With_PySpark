# importing modules
import sys, os, boto3
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum
from awsglue.utils import getResolvedOptions

s3_client = boto3.client("s3")
sns_client = boto3.client('sns')
glue_client = boto3.client('glue')

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'csv_file', 'parquet_file', 'file', 'query', 'table_name', 'sns_topic_arn', 'crawler_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
glueContext.setConf("spark.sql.streaming.checkpointLocation", "")
spark = glueContext.spark_session
spark.conf.set("fs.s3.maxConnections", "100")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['bucket_name']
csv_file_path = args['csv_file']
parquet_file_path = args['parquet_file']
file = args['file']
query = args['query']
table_name = args['table_name']

def send_email_notification(subject, message):
    try:
        sns_topic_arn = args['sns_topic_arn']
        msg = message
        sub = subject
        response = sns_client.publish(TopicArn=sns_topic_arn, Message=msg, Subject=sub)
    except Exception as e:
        print("Failed to send SNS notification: ", e)

def delete_existing_file():
    try:
        s3_client.delete_object(Bucket=bucket_name, Key='output/result.csv')
        s3_client.delete_object(Bucket=bucket_name, Key='output/result.parquet')
    except Exception as e:
        print("Delete exiting file error : ", e)
        send_email_notification("Execution failed status", f"Dear Team,\nThe delete exiting file error\n{e}\n\nThanks!")

def rename_the_file(extention):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="output/")
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith(".csv") or key.endswith(".parquet"):
                copy_source = {'Bucket': bucket_name, 'Key': key}
                s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=f"output/result.{extention}")
                s3_client.delete_object(Bucket=bucket_name, Key=key)
    except Exception as e:
        print("File Rename Error : ", e)
        send_email_notification("Execution failed status", f"Dear Team,\nThe File Rename Function Error\n{e}\n\nThanks!")

if file == 'csv':
    print("Reading csv file.")
    path = f"s3://{bucket_name}/input_data/{file}/{csv_file_path}"
    #df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path]},
        format="csv",
        format_options={"withHeader": True, "separator": ","}
        )
elif file == 'parquet':
    print("Reading parquet file")
    path = f"s3://{bucket_name}/input_data/{file}/{parquet_file_path}"
    # df = spark.read.parquet(path)
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path]},
        format="parquet"
        )
else:
    print(f"Unsupported file format: {file}. Supported formats are 'csv', 'parquet' Exiting.")
    send_email_notification("Execution failed status", f"Dear Team,\nNUnsupported file format: {file}. Supported formats are 'csv', 'parquet' Exiting.\n\nThanks!")
    sys.exit(1)

df = dynamic_frame.toDF()

df = df.fillna("Nan")    
    
print("Display the structure of the DataFrame:\n", df.printSchema())

row_count = df.count()
print(f"Total Rows: {row_count}")

column_count = len(df.columns)
print(f"Total Columns: {column_count}")

columns = df.columns
print(f"Columns: {columns}")

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
print("Null counts:\n", null_counts.show())

duplicate_count = df.count() - df.distinct().count()
print(f"Number of duplicate rows: {duplicate_count}")

df.createOrReplaceTempView(table_name)
print("Query is : ", query)

result_df = spark.sql(query)

delete_existing_file()

output_path = f"s3://{bucket_name}/output/"

if file == 'csv':
    print("Write data in to parquet format.")
    #result_df.coalesce(1).write.mode("overwrite").parquet(output_path)
    #rename_the_file('parquet')
    result_df.write.option("compression", "snappy").mode("overwrite").parquet(output_path)
elif file == 'parquet':
    print("Write data in to CSV format.")
    # result_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
    # rename_the_file('csv')
    result_df.write.option("header", "true").mode("overwrite").csv(output_path)
else:
    print(f"No file format: {file}. Supported formats are 'csv', 'parquet' Exiting.")
    send_email_notification("Execution failed status", f"Dear Team,\nNo file format: {file}. Supported formats are 'csv', 'parquet' Exiting.\n\nThanks!")
    sys.exit(1)

try:
    response = glue_client.start_crawler(Name=args['crawler_name'])
except glue_client.exceptions.CrawlerRunningException:
    print(f"Crawler '{crawler_name}' is already running.")
except Exception as e:
    print(f"An error occurred: {str(e)}")
    send_email_notification("Execution failed status", f"Dear Team,\nThe execution has failed. Please check the below error \n {e}.\n\nThanks!")    

send_email_notification("Execution successfully completed status", "Dear Team,\nThe execution has been successfully completed. Please check the detailed result in Athena.\n\nThanks!")  

print("Execution Completed....")
job.commit()