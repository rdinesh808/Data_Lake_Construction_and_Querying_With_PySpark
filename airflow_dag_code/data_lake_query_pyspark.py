import boto3
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "start_date": datetime(2024, 11, 20),
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

dag = DAG(
    dag_id="data_lake_construction_query_pyspark",
    default_args=default_args,
    schedule_interval="0 * * * 1-5",
    catchup=False,
    tags=["data_lake", "pyspark"]
)

def start_execution():
    print("Starting execution...!!!")

def finish_execution():
    print("Execution finished...!!!")

def execute_glue_job():
    try:
        lambda_client = boto3.client("lambda", region_name="us-east-1")
        res = lambda_client.invoke(FunctionName='aws-hackathon-lambda-data-lake-querying-pyspark ', InvocationType='RequestResponse', LogType='Tail')
        return res
    except Exception as e:
        print(e)

execution_start_task = PythonOperator(
    task_id="execution_start",
    python_callable=start_execution,
    dag=dag
)

execution_finish_task = PythonOperator(
    task_id="execution_finish",
    python_callable=finish_execution,
    dag=dag
)

execute_glue_job_task = PythonOperator(
    task_id="execute_glue_job",
    python_callable=execute_glue_job,
    dag=dag
)
execution_start_task >> execute_glue_job_task >> execution_finish_task