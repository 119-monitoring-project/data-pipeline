from kafka.producer import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime
import sys
import os
import xmltodict
import json
import pymysql
import asyncio
import aiohttp

load_dotenv()

host = os.getenv('HOST')
port = os.getenv('PORT')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
api_key = os.getenv('API_KEY')
broker = (os.getenv('BROKER1'), os.getenv('BROKER2'), os.getenv('BROKER3'))


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


async def get_api_data(location, session):
    params = {'serviceKey': api_key, 'pageNo': '1', 'numOfRows': '100', 'STAGE1': location}
    text = ''
    try:
        async with session.get(f'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire', params=params) as response:
            response = await response.text()
            jsonString = json.dumps(xmltodict.parse(response), indent=4)

            json_data = json.loads(jsonString)['response']['body']['items']
            if json_data is not None:
                for item in json_data['item']:
                    text += str(item) + '\n'
    except Exception as e:
        print('error', e, location)
        return 'error'
        pass

    return text


async def main():
    query = "SELECT L1 " \
            "FROM LOCATIONS " \
            "GROUP BY L1"

    cursor.execute(query)
    locations = cursor.fetchall()
    producer = get_producer(broker)

    async with aiohttp.ClientSession() as session:
        while True:
            send_time = datetime.now()
            tasklist = [asyncio.ensure_future(get_api_data(location, session)) for location in locations]
            results = await asyncio.gather(*tasklist)
            text = ''
            error_flag = False
            for result in results:
                if result == 'error':
                    error_flag = True
                    break
                result = result.replace("'", '"')
                text += result
            if error_flag:
                continue

            producer.send(
                key=str(send_time).encode('utf-8'),
                topic='emergency_data',
                value=text.encode('utf-8')
            )

            producer.flush()
            print('전송완료', send_time)

asyncio.run(main())
