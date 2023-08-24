import os
import boto3
import pymysql
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from dotenv import load_dotenv

load_dotenv()

BROKERS = (os.getenv('BROKER1'), os.getenv('BROKER2'), os.getenv('BROKER3'))
consumer_group_id = "emergency_consumer"
host = os.getenv('HOST')
port = os.getenv('PORT')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
ACCESS_ID = os.getenv('AWS_S3_ACCESS_ID')
ACCESS_KEY = os.getenv('AWS_S3_ACCESS_KEY')
REGION = os.getenv('AWS_REGION')
PATH = os.getenv('LOCAL_S3_DATA_FOLDER_PATH')

try:
    # DB Connection 생성
    conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
    cursor = conn.cursor()

except Exception as e:
    print(e)


def get_s3_client():
    s3_client = boto3.client('s3',
                             aws_access_key_id=ACCESS_ID,
                             aws_secret_access_key=ACCESS_KEY,
                             region_name=REGION
                             )

    return s3_client


def upload_file_to_s3(file_name, object_name=None):
    s3_client = get_s3_client()

    bucket_name = 'de-5-1'
    file_path = f'{PATH}{file_name}'

    s3_client.upload_file(file_path, bucket_name, 'real_time_data/' + file_name)


def get_latest_file_name_from_s3(bucket_name):
    s3_client = get_s3_client()

    obj_list = s3_client.list_objects(Bucket=bucket_name)
    contents_list = obj_list['Contents']

    key_list = [x['Key'] for x in contents_list]
    return sorted(key_list)[-1]


def download_file_from_s3(file_name, bucket_name):
    s3_client = get_s3_client()
    obj = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_name

    )
    latest_file = obj["Body"].read().decode('utf-8')

    return latest_file


consumer = KafkaConsumer(
    bootstrap_servers=BROKERS,
    group_id=consumer_group_id,
    value_deserializer=lambda x: x.decode('utf-8'),
    auto_offset_reset='latest',
    consumer_timeout_ms=60000,
    enable_auto_commit=False)

consumer.subscribe('emergency_data')

latest_file_name = get_latest_file_name_from_s3('de-5-1')
latest_file = download_file_from_s3(latest_file_name, 'de-5-1')


for record in consumer:
    topic_partition = TopicPartition(record.topic, record.partition)
    offset = OffsetAndMetadata(record.offset + 1, record.timestamp)
    consumer.commit({
        topic_partition: offset
    })

    now = datetime.now()
    if latest_file != record.value:
        with open(f's3_data/{now}.json', 'w') as file:
            file.write(record.value)
        file.close()
        file_name = f'{now}' + '.json'
        upload_file_to_s3(file_name)

        latest_file = record.value

        query = f"UPDATE S3_PATHS SET latest_reaL_time_file_path = 'real_time_data/{file_name}', current_update = NOW()"
        print(query)
        cursor.execute(query)
        conn.commit()

consumer.close()
