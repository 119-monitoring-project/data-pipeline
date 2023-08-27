import requests
import xmltodict
import logging
from module.util.connector.rds import ConnectDB
from module.util.preprocessor.query import InsertQuery

from concurrent.futures import ThreadPoolExecutor
from airflow.models import Variable

class LoadHpidInfo:
    # def __init__(self, url, center_type, service_key):
    #     self.url = url
    #     self.center_type = center_type
    #     self.service_key = service_key

    def CallAPI(op_orgs, **kwargs):
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
    def LoadBasicInfo(**kwargs):
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        
        upstream_task_id = list(kwargs['ti'].task.upstream_task_ids)[0]
        data = kwargs['ti'].xcom_pull(key=upstream_task_id)

        hpids = InsertQuery().InsertBasicInfoQuery(data, execution_date)
        
        kwargs['ti'].xcom_push(key='load_hpids', value=hpids)

    def LoadDetailInfo(self, hpids, url, execution_date):
        logging.info(f"쓰레드가 시작되었습니다.")
        
        servicekey = Variable.get('SERVICEKEY')

        retry_hpids = []  # http 오류가 난 API는 재호출 시도
        for hpid in list(hpids):
            params = {'serviceKey': servicekey, 'HPID': hpid, 'pageNo': '1', 'numOfRows': '9999'}

            try:
                response = requests.get(url, params=params)
                xmlString = response.text
                jsonString = xmltodict.parse(xmlString)
                data = jsonString['response']['body']['items']['item']
                InsertQuery().InsertDetailInfoQuery(data, execution_date)
            except :
                logging.info(f"{hpid}가 예외되었습니다.")
                retry_hpids.append(hpid)
                continue

        logging.info(f"쓰레드가 종료되었습니다.")
        
    def SaveConcurrentDB(self, url, **kwargs):
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
                self.LoadDetailInfo,
                chunk_1,
                url,
                execution_date
                )
            
            executor.submit(
                self.LoadDetailInfo,
                chunk_2,
                url,
                execution_date
                )
            
            executor.submit(
                self.LoadDetailInfo,
                chunk_3,
                url,
                execution_date
                )
            
            executor.submit(
                self.LoadDetailInfo,
                chunk_4,
                url,
                execution_date
                )
    
    def ReloadDetailInfo(**kwargs):
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        retry_hpids = kwargs['ti'].xcom_pull(key='retry_hpids')

        servicekey = Variable.get('SERVICEKEY')

        conn, _ = ConnectDB()

        for data in list(retry_hpids):
            hpid = data[0]
            center_type = data[1]

            if center_type == '0':
                url = Variable.get('DETAIL_EGYT_URL')
            elif center_type == '1':
                url = Variable.get('DETAIL_STRM_URL')

            params = {'serviceKey': servicekey, 'HPID':hpid, 'pageNo' : '1', 'numOfRows' : '9999'}

            try:
                response = requests.get(url, params=params, timeout=100)
                xmlString = response.text
                jsonString = xmltodict.parse(xmlString)             
                data = jsonString['response']['body']['items']['item']
                InsertQuery().InsertDetailInfoQuery(data, execution_date)
            except:
                print("slack ! ! !")
                continue

        conn.commit() 