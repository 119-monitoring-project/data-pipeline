import requests
import xmltodict
import logging
from module.util.connector.rds import ConnectDB
from module.util.preprocessor.query import InsertQuery

from airflow.models import Variable

# DB(rds)에 hpid 정보를 적재
class LoadHpidInfo:
    # 해당 url에 대한 api를 호출하여 data 수집
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

        # center_type 추가 (egyt==0, strm==1)
        for duty in data:
            duty.update({'center_type': center_type})

        kwargs['ti'].xcom_push(key=current_task_name, value=data)

    # basic info 적재
    def LoadBasicInfo(**kwargs):
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        
        current_task_id = kwargs['ti'].task_id
        upstream_task_id = list(kwargs['ti'].task.upstream_task_ids)[0]
        data = kwargs['ti'].xcom_pull(key=upstream_task_id)

        hpids = InsertQuery().InsertBasicInfoQuery(data, execution_date)
        
        kwargs['ti'].xcom_push(key=current_task_id, value=hpids)

    # detail info 적재
    def LoadDetailInfo(hpids, url, execution_date):
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
        
    # 1차 적재가 되지 않은 detail info에 대해 재적재
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
                raise Exception(f"fail hpid: {hpid}")

        conn.commit() 