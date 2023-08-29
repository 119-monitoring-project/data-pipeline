import boto3
from airflow.models import Variable

def ConnectS3():
    s3_client = boto3.client('s3',
                            aws_access_key_id=Variable.get('aws_secret_access_id'),
                            aws_secret_access_key=Variable.get('aws_secret_access_key'),
                            region_name=Variable.get('aws_region')
                            )

    return s3_client