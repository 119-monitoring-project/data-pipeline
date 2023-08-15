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
api_key = os.getenv('API_KEY')

try:
    # DB Connection 생성
    conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
    cursor = conn.cursor()

except Exception as e:
    print(e)


def insert_into_hospital_detail_info(url, params, center_type):
    select_query = f"SELECT hpid FROM HOSPITAL_BASIC_INFO WHERE center_type = {center_type}"
    cursor.execute(select_query)
    hpids = cursor.fetchall()
    retry_hpids = []

    for hpid in hpids:
        params['HPID'] = hpid
        response = requests.get(url, params=params)
        xmlString = response.content
        jsonString = json.dumps(xmltodict.parse(xmlString), indent=4)
        try:
            data = json.loads(jsonString)['response']['body']['items']['item']
        except:
            retry_hpids.append(hpid)

        hpid = data.get('hpid', '')
        post_cdn1 = data.get('postCdn1', '')
        post_cdn2 = data.get('postCdn2', '')
        hvec = data.get('hvec', '')
        hvoc = data.get('hvoc', '')
        hvcc = data.get('hvcc', '')
        hvncc = data.get('hvncc', '')
        hvccc = data.get('hvccc', '')
        hvicc = data.get('hvicc', '')
        hvgc = data.get('hvgc', '')
        duty_hayn = data.get('dutyHayn', '')
        duty_hano = data.get('dutyHano', '')
        duty_inf = data.get('dutyInf', '')
        if duty_inf is None:
            duty_inf = ''
        else:
            duty_inf = duty_inf.replace('\'', '\'\'')
        duty_map_img = data.get('dutyMapimg', '')
        if duty_map_img is None:
            duty_map_img = ''
        else:
            duty_map_img = duty_map_img.replace('\'', '\'\'')
        duty_eryn = data.get('dutyEryn', '')
        duty_time_1c = data.get('dutyTime1c', '')
        duty_time_2c = data.get('dutyTime2c', '')
        duty_time_3c = data.get('dutyTime3c', '')
        duty_time_4c = data.get('dutyTime4c', '')
        duty_time_5c = data.get('dutyTime5c', '')
        duty_time_6c = data.get('dutyTime6c', '')
        duty_time_7c = data.get('dutyTime7c', '')
        duty_time_8c = data.get('dutyTime8c', '')
        duty_time_1s = data.get('dutyTime1c', '')
        duty_time_2s = data.get('dutyTime2c', '')
        duty_time_3s = data.get('dutyTime3c', '')
        duty_time_4s = data.get('dutyTime4c', '')
        duty_time_5s = data.get('dutyTime5c', '')
        duty_time_6s = data.get('dutyTime6c', '')
        duty_time_7s = data.get('dutyTime7c', '')
        duty_time_8s = data.get('dutyTime8c', '')
        mkioskty25 = data.get('MKioskTy25', '')
        mkioskty1 = data.get('MKioskTy1', '')
        mkisokty2 = data.get('MKioskTy2', '')
        mkisokty3 = data.get('MKioskTy3', '')
        mkisokty4 = data.get('MKioskTy4', '')
        mkisokty5 = data.get('MKioskTy5', '')
        mkisokty6 = data.get('MKioskTy6', '')
        mkisokty7 = data.get('MKioskTy7', '')
        mkisokty8 = data.get('MKioskTy8', '')
        mkisokty9 = data.get('MKioskTy9', '')
        mkisokty10 = data.get('MKioskTy10', '')
        mkisokty11 = data.get('MKioskTy11', '')
        dgid_id_name = data.get('dgidIdName', '')
        hpbdn = data.get('hpbdn', '')
        hpccuyn = data.get('hpccuyn', '')
        hpcuyn = data.get('hpcuyn', '')
        hperyn = data.get('hperyn', '')
        hpgryn = data.get('hpgryn', '')
        hpicuyn = data.get('hpicuyn', '')
        hpnicuyn = data.get('hpnicuyn', '')
        hpopyn = data.get('hpopyn', '')

        query = f"INSERT INTO HOSPITAL_DETAIL_INFO (hpid, post_cdn1, post_cdn2, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, hvgc, duty_hayn, duty_hano, duty_inf, duty_map_img, duty_eryn, duty_time_1c, duty_time_2c, duty_time_3c, duty_time_4c, duty_time_5c, duty_time_6c, duty_time_7c, duty_time_8c, duty_time_1s, duty_time_2s, duty_time_3s, duty_time_4s, duty_time_5s, duty_time_6s, duty_time_7s, duty_time_8s, mkioskty25, mkioskty1, mkisokty2, mkisokty3, mkisokty4, mkisokty5, mkisokty6, mkisokty7, mkisokty8, mkisokty9, mkisokty10, mkisokty11, dgid_id_name, hpbdn, hpccuyn, hpcuyn, hperyn, hpgryn, hpicuyn, hpnicuyn, hpopyn) VALUES " \
                "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')" \
            .format(hpid, post_cdn1, post_cdn2, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, hvgc, duty_hayn, duty_hano,
                    duty_inf, duty_map_img, duty_eryn, duty_time_1c, duty_time_2c, duty_time_3c, duty_time_4c,
                    duty_time_5c, duty_time_6c, duty_time_7c, duty_time_8c, duty_time_1s, duty_time_2s,
                    duty_time_3s, duty_time_4s, duty_time_5s, duty_time_6s, duty_time_7s, duty_time_8s, mkioskty25,
                    mkioskty1, mkisokty2, mkisokty3, mkisokty4, mkisokty5, mkisokty6, mkisokty7, mkisokty8,
                    mkisokty9, mkisokty10, mkisokty11, dgid_id_name, hpbdn, hpccuyn, hpcuyn, hperyn, hpgryn,
                    hpicuyn, hpnicuyn, hpopyn)

        cursor.execute(query)

    conn.commit()

    for hpid in retry_hpids:
        params['HPID'] = hpid
        response = requests.get(url, params=params)
        xmlString = response.content
        jsonString = json.dumps(xmltodict.parse(xmlString), indent=4)

        try:
            data = json.loads(jsonString)['response']['body']['items']['item']
        except:
            print('error hpid : ', hpid)

        hpid = data.get('hpid', '')
        post_cdn1 = data.get('postCdn1', '')
        post_cdn2 = data.get('postCdn2', '')
        hvec = data.get('hvec', '')
        hvoc = data.get('hvoc', '')
        hvcc = data.get('hvcc', '')
        hvncc = data.get('hvncc', '')
        hvccc = data.get('hvccc', '')
        hvicc = data.get('hvicc', '')
        hvgc = data.get('hvgc', '')
        duty_hayn = data.get('dutyHayn', '')
        duty_hano = data.get('dutyHano', '')
        duty_inf = data.get('dutyInf', '')
        if duty_inf is None:
            duty_inf = ''
        else:
            duty_inf = duty_inf.replace('\'', '\'\'')
        duty_map_img = data.get('dutyMapimg', '')
        if duty_map_img is None:
            duty_map_img = ''
        else:
            duty_map_img = duty_map_img.replace('\'', '\'\'')
        duty_eryn = data.get('dutyEryn', '')
        duty_time_1c = data.get('dutyTime1c', '')
        duty_time_2c = data.get('dutyTime2c', '')
        duty_time_3c = data.get('dutyTime3c', '')
        duty_time_4c = data.get('dutyTime4c', '')
        duty_time_5c = data.get('dutyTime5c', '')
        duty_time_6c = data.get('dutyTime6c', '')
        duty_time_7c = data.get('dutyTime7c', '')
        duty_time_8c = data.get('dutyTime8c', '')
        duty_time_1s = data.get('dutyTime1c', '')
        duty_time_2s = data.get('dutyTime2c', '')
        duty_time_3s = data.get('dutyTime3c', '')
        duty_time_4s = data.get('dutyTime4c', '')
        duty_time_5s = data.get('dutyTime5c', '')
        duty_time_6s = data.get('dutyTime6c', '')
        duty_time_7s = data.get('dutyTime7c', '')
        duty_time_8s = data.get('dutyTime8c', '')
        mkioskty25 = data.get('MKioskTy25', '')
        mkioskty1 = data.get('MKioskTy1', '')
        mkisokty2 = data.get('MKioskTy2', '')
        mkisokty3 = data.get('MKioskTy3', '')
        mkisokty4 = data.get('MKioskTy4', '')
        mkisokty5 = data.get('MKioskTy5', '')
        mkisokty6 = data.get('MKioskTy6', '')
        mkisokty7 = data.get('MKioskTy7', '')
        mkisokty8 = data.get('MKioskTy8', '')
        mkisokty9 = data.get('MKioskTy9', '')
        mkisokty10 = data.get('MKioskTy10', '')
        mkisokty11 = data.get('MKioskTy11', '')
        dgid_id_name = data.get('dgidIdName', '')
        hpbdn = data.get('hpbdn', '')
        hpccuyn = data.get('hpccuyn', '')
        hpcuyn = data.get('hpcuyn', '')
        hperyn = data.get('hperyn', '')
        hpgryn = data.get('hpgryn', '')
        hpicuyn = data.get('hpicuyn', '')
        hpnicuyn = data.get('hpnicuyn', '')
        hpopyn = data.get('hpopyn', '')

        query = f"INSERT INTO HOSPITAL_DETAIL_INFO (hpid, post_cdn1, post_cdn2, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, hvgc, duty_hayn, duty_hano, duty_inf, duty_map_img, duty_eryn, duty_time_1c, duty_time_2c, duty_time_3c, duty_time_4c, duty_time_5c, duty_time_6c, duty_time_7c, duty_time_8c, duty_time_1s, duty_time_2s, duty_time_3s, duty_time_4s, duty_time_5s, duty_time_6s, duty_time_7s, duty_time_8s, mkioskty25, mkioskty1, mkisokty2, mkisokty3, mkisokty4, mkisokty5, mkisokty6, mkisokty7, mkisokty8, mkisokty9, mkisokty10, mkisokty11, dgid_id_name, hpbdn, hpccuyn, hpcuyn, hperyn, hpgryn, hpicuyn, hpnicuyn, hpopyn) VALUES " \
                "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')" \
            .format(hpid, post_cdn1, post_cdn2, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, hvgc, duty_hayn, duty_hano,
                    duty_inf, duty_map_img, duty_eryn, duty_time_1c, duty_time_2c, duty_time_3c, duty_time_4c,
                    duty_time_5c, duty_time_6c, duty_time_7c, duty_time_8c, duty_time_1s, duty_time_2s,
                    duty_time_3s, duty_time_4s, duty_time_5s, duty_time_6s, duty_time_7s, duty_time_8s, mkioskty25,
                    mkioskty1, mkisokty2, mkisokty3, mkisokty4, mkisokty5, mkisokty6, mkisokty7, mkisokty8,
                    mkisokty9, mkisokty10, mkisokty11, dgid_id_name, hpbdn, hpccuyn, hpcuyn, hperyn, hpgryn,
                    hpicuyn, hpnicuyn, hpopyn)

        cursor.execute(query)

    conn.commit()

emergency_hospital_url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytBassInfoInqire'
emergency_hospital_params = {'serviceKey': api_key, 'pageNo': '1', 'numOfRows': '1' }

insert_into_hospital_detail_info(emergency_hospital_url, emergency_hospital_params, 0)

trauma_hospital_url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getStrmBassInfoInqire'
emergency_hospital_params = {'serviceKey': api_key, 'pageNo': '1', 'numOfRows': '1' }

insert_into_hospital_detail_info(trauma_hospital_url, emergency_hospital_params, 1)
