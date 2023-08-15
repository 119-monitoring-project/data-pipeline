from kafka.producer import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime
import os
import xmltodict
import json
import pymysql
import requests

load_dotenv()

host = os.getenv('HOST')
port = os.getenv('PORT')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
api_key = os.getenv('API_KEY')


try:
    # DB Connection 생성
    conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
    cursor = conn.cursor()

except Exception as e:
    print(e)


def get_producer(bootstrap_servers: tuple):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="emergency_producer",
        acks=1
    )
    return producer


if __name__ == '__main__':
    BROKERS = ('localhost:9092', 'localhost:9093', 'localhost:9094')

    temp_text = ''

    query = "SELECT L1 " \
            "FROM LOCATIONS " \
            "GROUP BY L1"

    cursor.execute(query)
    data = cursor.fetchall()
    url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire'
    producer = get_producer(BROKERS)
    while True:
        text = ''
        for x in data:
            params = {'serviceKey': api_key, 'pageNo': '1', 'numOfRows': '100', 'STAGE1': x[0]}

            response = requests.get(url, params=params)
            xmlString = response.content
            jsonString = json.dumps(xmltodict.parse(xmlString), indent=4)
            json_data = json.loads(jsonString)['response']['body']['items']
            if json_data is not None:
                for item in json_data['item']:
                    text += str(item) + ' '
        if temp_text != text:
            print(f'{datetime.now()} : 바뀜')
        temp_text = text

        # producer.send(
        #     topic='emergency_data',
        #     value=text.encode('utf-8')
        # )
        # producer.flush()
