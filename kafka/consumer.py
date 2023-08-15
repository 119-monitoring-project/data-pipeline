import os
import boto3
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from dotenv import load_dotenv

load_dotenv()

BROKERS = ('localhost:9092', 'localhost:9093', 'localhost:9094')
consumer_group_id = "emergency_consumer"
ACCESS_ID = os.getenv('AWS_S3_ACCESS_ID')
ACCESS_KEY = os.getenv('AWS_S3_ACCESS_KEY')
REGION = os.getenv('AWS_REGION')
PATH = os.getenv('LOCAL_S3_DATA_FOLDER_PATH')


def upload_file_to_s3(file_name, object_name=None):
    bucket_name = 'de-5-1'
    file_path = f'{PATH}{file_name}'

    # S3 클라이언트 생성
    s3_client = boto3.client('s3',
                             aws_access_key_id=ACCESS_ID,
                             aws_secret_access_key=ACCESS_KEY,
                             region_name=REGION
                             )

    s3_client.upload_file(file_path, bucket_name, 'real_time_data/'+file_name)


consumer = KafkaConsumer(
    bootstrap_servers=BROKERS,
    group_id=consumer_group_id,
    value_deserializer=lambda x: x.decode('utf-8'),
    auto_offset_reset='latest',
    consumer_timeout_ms=30000,
    enable_auto_commit=False)

consumer.subscribe('emergency_data')

for record in consumer:
    print(f"""{record.topic}: 데이터 {record.value} 를 파티션 {record.partition} 의 {record.offset} 번 오프셋에서 읽어옴""")
    topic_partition = TopicPartition(record.topic, record.partition)
    offset = OffsetAndMetadata(record.offset + 1, record.timestamp)
    consumer.commit({
        topic_partition: offset
    })

    now = datetime.now()
    with open(f's3_data/{now}.json', 'w') as file:
        file.write(record.value)
    file.close()

    upload_file_to_s3(f'{now}'+'.json')

consumer.close()

