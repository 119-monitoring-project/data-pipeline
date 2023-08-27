import requests
import xmltodict
from airflow.models import Variable
from module.util.db_connecting import connect_db
from concurrent.futures import ThreadPoolExecutor
import logging

def insert_hpid_info(data, cursor, execution_date):
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
    mkioskty2 = data.get('MKioskTy2', '')
    mkioskty3 = data.get('MKioskTy3', '')
    mkioskty4 = data.get('MKioskTy4', '')
    mkioskty5 = data.get('MKioskTy5', '')
    mkioskty6 = data.get('MKioskTy6', '')
    mkioskty7 = data.get('MKioskTy7', '')
    mkioskty8 = data.get('MKioskTy8', '')
    mkioskty9 = data.get('MKioskTy9', '')
    mkioskty10 = data.get('MKioskTy10', '')
    mkioskty11 = data.get('MKioskTy11', '')
    dgid_id_name = data.get('dgidIdName', '')
    hpbdn = data.get('hpbdn', '')
    hpccuyn = data.get('hpccuyn', '')
    hpcuyn = data.get('hpcuyn', '')
    hperyn = data.get('hperyn', '')
    hpgryn = data.get('hpgryn', '')
    hpicuyn = data.get('hpicuyn', '')
    hpnicuyn = data.get('hpnicuyn', '')
    hpopyn = data.get('hpopyn', '')
    dt = execution_date

    query = f"INSERT INTO HOSPITAL_DETAIL_INFO (hpid, post_cdn1, post_cdn2, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, hvgc, duty_hayn, duty_hano, duty_inf, duty_map_img, duty_eryn, duty_time_1c, duty_time_2c, duty_time_3c, duty_time_4c, duty_time_5c, duty_time_6c, duty_time_7c, duty_time_8c, duty_time_1s, duty_time_2s, duty_time_3s, duty_time_4s, duty_time_5s, duty_time_6s, duty_time_7s, duty_time_8s, mkioskty25, mkioskty1, mkioskty2, mkioskty3, mkioskty4, mkioskty5, mkioskty6, mkioskty7, mkioskty8, mkioskty9, mkioskty10, mkioskty11, dgid_id_name, hpbdn, hpccuyn, hpcuyn, hperyn, hpgryn, hpicuyn, hpnicuyn, hpopyn, dt) VALUES " \
            "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')" \
        .format(hpid, post_cdn1, post_cdn2, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, hvgc, duty_hayn, duty_hano,
                duty_inf, duty_map_img, duty_eryn, duty_time_1c, duty_time_2c, duty_time_3c, duty_time_4c,
                duty_time_5c, duty_time_6c, duty_time_7c, duty_time_8c, duty_time_1s, duty_time_2s,
                duty_time_3s, duty_time_4s, duty_time_5s, duty_time_6s, duty_time_7s, duty_time_8s, mkioskty25,
                mkioskty1, mkioskty2, mkioskty3, mkioskty4, mkioskty5, mkioskty6, mkioskty7, mkioskty8,
                mkioskty9, mkioskty10, mkioskty11, dgid_id_name, hpbdn, hpccuyn, hpcuyn, hperyn, hpgryn,
                hpicuyn, hpnicuyn, hpopyn, dt)
    print(query)
    
    cursor.execute(query)

class DataLoader:
    # def __init__(self, url, center_type, service_key):
    #     self.url = url
    #     self.center_type = center_type
    #     self.service_key = service_key

    def call_api(op_orgs, **kwargs):
        url = op_orgs[0]
        center_type = op_orgs[1]
        current_task_name = kwargs['task_instance'].task_id

        servicekey = Variable.get('SERVICEKEY')
        params = {'serviceKey': servicekey, 'pageNo' : '1', 'numOfRows' : '9999' }

        response = requests.get(url, params=params)
        xmlString = response.text
        jsonString = xmltodict.parse(xmlString)

        data = jsonString['response']['body']['items']['item']

        # center_type 추가 
        for duty in data:
            duty.update({'center_type': center_type})

        kwargs['ti'].xcom_push(key=current_task_name, value=data)

    # 데이터 적재
    def load_basic_data_to_rds(**kwargs):
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')

        conn, cursor = connect_db()
        
        upstream_task_id = list(kwargs['ti'].task.upstream_task_ids)[0]
        data = kwargs['ti'].xcom_pull(key=upstream_task_id)
        # 병뭔 목록 저장
        hpids = []

        # 데이터 적재
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
            center_type = x.get('center_type', '')
            dt = execution_date

            query = f"INSERT INTO HOSPITAL_BASIC_INFO (hpid, phpid, duty_emcls, duty_emcls_name, duty_addr, duty_name, duty_tel1, duty_tel3, wgs_84_lon, wgs_84_lat, center_type, dt)" \
                    f" VALUES ('{hpid}', '{phpid}', '{duty_emcls}', '{duty_emcls_name}', '{duty_addr}', '{duty_name}', '{duty_tel1}', '{duty_tel3}', '{wgs_84_lon}', '{wgs_84_lat}', '{center_type}', '{dt}')"
            print(query)

            hpids.append(hpid) # 리스트에 hpid 저장

            cursor.execute(query)
        conn.commit() 
        
        kwargs['ti'].xcom_push(key='load_hpids', value=hpids)

    def load_detail_data_to_rds(self, hpids, url, execution_date):
        # rds에 적재된 의료기관의 hpid
        logging.info(f"쓰레드가 시작되었습니다.")
        
        servicekey = Variable.get('SERVICEKEY')
        
        conn, cursor = connect_db()

        retry_hpids = []  # http 오류가 난 API는 재호출 시도
        for hpid in list(hpids):
            params = {'serviceKey': servicekey, 'HPID': hpid, 'pageNo': '1', 'numOfRows': '9999'}

            try:
                response = requests.get(url, params=params)
                xmlString = response.text
                jsonString = xmltodict.parse(xmlString)
                data = jsonString['response']['body']['items']['item']
                insert_hpid_info(data, cursor, execution_date)
            except :
                logging.info(f"{hpid}가 예외되었습니다.")
                retry_hpids.append(hpid)
                continue

        conn.commit()  # 1차로 적재 완료된 데이터에 대해 commit 진행
        logging.info(f"쓰레드가 종료되었습니다.")
        
    def concurrent_db_saving(self, url, **kwargs):
        hpids = kwargs['ti'].xcom_pull(key='load_hpids') 
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        
        workers = 4

        chunk_size = len(hpids) // workers
        chunks = [hpids[i:i+chunk_size] for i in range(0, len(hpids), chunk_size)]

        chunk_1 = chunks[0]
        chunk_2 = chunks[1]
        chunk_3 = chunks[2]
        chunk_4 = chunks[3]

        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.submit(
                self.load_detail_data_to_rds,
                chunk_1,
                url,
                execution_date
                )
            
            executor.submit(
                self.load_detail_data_to_rds,
                chunk_2,
                url,
                execution_date
                )
            
            executor.submit(
                self.load_detail_data_to_rds,
                chunk_3,
                url,
                execution_date
                )
            
            executor.submit(
                self.load_detail_data_to_rds,
                chunk_4,
                url,
                execution_date
                )