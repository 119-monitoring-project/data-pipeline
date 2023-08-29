import boto3
from airflow.models import Variable

def ConnectS3():
    s3_client = boto3.client('s3',
                            aws_access_key_id=Variable.get('AWS_ACCESS_ID'),
                            aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
                            region_name=Variable.get('AWS_REGION')
                            )

    return s3_client