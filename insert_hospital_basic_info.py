import requests
import json
import pymysql
import xmltodict
import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
host = os.getenv('HOST')
port = os.getenv('PORT')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

try:
    # DB Connection 생성
    conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
    cursor = conn.cursor()

except Exception as e:
    print(e)


def insert_into_hospital_basic_info(url, params, center_type):
    for x in data:
        duty_addr = x.get('dutyAddr' , '')
        duty_emcls = x.get('dutyEmcls', '')
        duty_emcls_name = x.get('dutyEmclsName', '')
        duty_name = x.get('dutyName', '')
        duty_tel1 = x.get('dutyTel1', '')
        duty_tel3 = x.get('dutyTel3', '')
        hpid = x.get('hpid', '')
        phpid = x.get('phpid', '')
        wgs_84_lat = x.get('wgs84Lat', '')
        wgs_84_lon = x.get('wgs84Lon', '')

        query = f"INSERT INTO HOSPITAL_BASIC_INFO (hpid, phpid, duty_emcls, duty_emcls_name, duty_addr, duty_name, duty_tel1, duty_tel3, wgs_84_lon, wgs_84_lat, center_type)" \
                f" VALUES ('{hpid}', '{phpid}', '{duty_emcls}', '{duty_emcls_name}', '{duty_addr}', '{duty_name}', '{duty_tel1}', '{duty_tel3}', '{wgs_84_lon}', '{wgs_84_lat}', '{center_type}')"
        print(query)
        #cursor.execute(query)
    conn.commit()


emergency_hospital_url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytListInfoInqire'
emergency_hospital_params = {'serviceKey' : 'MU0Pzy/M1ga8fgWxnhtbO7aKRlUbCzqBOwwjRtA2IgMD7qlMEhi7d7ojiOwcTuHVOXFRAJxJ0TVm77XzjAFtLw==', 'pageNo' : '1', 'numOfRows' : '9999' }

insert_into_hospital_basic_info(emergency_hospital_url, emergency_hospital_params, 0)

trauma_hospital_url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getStrmListInfoInqire'
emergency_hospital_params = {'serviceKey' : 'MU0Pzy/M1ga8fgWxnhtbO7aKRlUbCzqBOwwjRtA2IgMD7qlMEhi7d7ojiOwcTuHVOXFRAJxJ0TVm77XzjAFtLw==', 'pageNo' : '1', 'numOfRows' : '9999' }

insert_into_hospital_basic_info(trauma_hospital_url, emergency_hospital_params, 1)
