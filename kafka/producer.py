from kafka.producer import KafkaProducer
from dotenv import load_dotenv
import sys
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


def deep_getsizeof(obj, seen=None) -> int:
    """재귀적으로 객체의 메모리 사용량을 계산하는 함수"""
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # 이미 본 객체는 저장
    seen.add(obj_id)
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        size += sum(deep_getsizeof(v, seen) for v in obj.values())
        size += sum(deep_getsizeof(k, seen) for k in obj.keys())
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum(deep_getsizeof(i, seen) for i in obj)
    return size


def get_producer(bootstrap_servers: tuple):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="emergency_producer",
        acks=1
    )
    return producer


if __name__ == '__main__':
    BROKERS = (os.getenv('BROKER1'), os.getenv('BROKER2'), os.getenv('BROKER3'))

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
        for i, x in enumerate(data):
            params = {'serviceKey': api_key, 'pageNo': '1', 'numOfRows': '100', 'STAGE1': x[0]}

            response = requests.get(url, params=params)
            xmlString = response.content
            jsonString = json.dumps(xmltodict.parse(xmlString), indent=4)
            try:
                json_data = json.loads(jsonString)['response']['body']['items']
                print(json_data)
                if json_data is not None:
                    for item in json_data['item']:
                        text += str(item) + '\n'
            except:
                print('error', x[0])
                break
                pass

        # print(deep_getsizeof(text.encode('utf-8')))
        text = text.replace("'", '"')
        producer.send(
            topic='emergency_data',
            value=text.encode('utf-8')
        )

        producer.flush()
        print('전송완료')