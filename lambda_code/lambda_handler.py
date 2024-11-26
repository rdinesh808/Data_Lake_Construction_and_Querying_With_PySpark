import os, json, boto3, time

s3 = boto3.client('s3')
glue = boto3.client('glue')

def unsaved_folder():
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=os.getenv("bucket_name"), Prefix="Unsaved"):
            if 'Contents' in page:
                keys = [{'Key': obj['Key']} for obj in page['Contents']]
                delete_response = s3.delete_objects(Bucket=os.getenv("bucket_name"), Delete={'Objects': keys, 'Quiet': True})
                print(f"Deleted batch of {len(keys)} objects.")
    except Exception as e:
        print(f"Error deleting folder : {str(e)}")

def lambda_handler(event, context):
    unsaved_folder()
    #time.sleep(3)
    try:
        s3_response = s3.get_object(Bucket=os.getenv('bucket_name'), Key=os.getenv('config_file_path'))
        json_data = json.loads(s3_response['Body'].read().decode('utf-8'))
        
        params = {
            "--file": json_data['file_type'],
            "--table_name": json_data['table_name'],
            "--query": json_data['ps_query']
        }
        print("Parameter is : ", params)
        glue_response = glue.start_job_run(
        JobName=os.getenv("glue_job_name"),
        Arguments=params )
        
    except Exception as e:
        print("Error lambda functions : ", e)
        return {
            'status_code': 400,
            'status': f'Failed lambda function trigger. {e}'
        }
    return {
        'status_code': 200,
        'status': 'Successfully GlueJob Triggered.'
    }