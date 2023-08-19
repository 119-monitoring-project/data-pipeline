"""Module providingFunction printing python version."""
from datetime import datetime, timedelta
import xmltodict
import requests
from packages.data_loader import insert_hpid_info, connect_db
from packages.data_loader import DataLoader
from packages.count_data_in_rds import count_data_in_rds
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable

def check_previous_task_result(**kwargs):
    previous_task_result = kwargs['ti'].xcom_pull(key='count_result')

    if previous_task_result == False:
        return 'reload_detail_data_to_rds'  # 결과가 False면 detail 재적재
    else:
        return 'finish_data_to_rds'  # 결과가 True면 rds to s3 진행

def reload_detail_data_to_rds(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    retry_hpids = kwargs['ti'].xcom_pull(key='retry_hpids')

    servicekey = Variable.get('SERVICEKEY')

    conn, cursor = connect_db()

### 태스크 그룹 만들기 
##### 쓰레드 풀로 수정 필요
    for data in list(retry_hpids):
        hpid = data[0]
        center_type = data[1]

        if center_type == '0':
            url = Variable.get('DETAIL_EGYT_URL')
        elif center_type == '1':
            url = Variable.get('DETAIL_STRM_URL')

        params = {'serviceKey': servicekey, 'HPID':hpid, 'pageNo' : '1', 'numOfRows' : '9999'}

        response = requests.get(url, params=params, timeout=100)
        xmlString = response.text
        jsonString = xmltodict.parse(xmlString)
        try:
            data = jsonString['response']['body']['items']['item']
            insert_hpid_info(data, cursor, execution_date)
        except:
            # 이 때는 진짜 슬랙 메세지 해야 함 ~
            print('슬메~')

    conn.commit() 


# DAG 설정
default_args = {
    'start_date': datetime(2023, 8, 9),
    'timezone': 'Asia/Seoul',
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}

with DAG(
    'api_to_rds_Dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    start_task = EmptyOperator(
        task_id='start_data_extraction'
    )

    op_orgs = [Variable.get('BASIC_EGYT_URL'), 0] # 응급의료기관은 center_type == 0
    call_basic_info_Egyt = PythonOperator(
        task_id='call_basic_Egyt_data',
        python_callable=DataLoader.call_api,
        op_args=[op_orgs],
        provide_context=True
    )

    op_orgs = [Variable.get('BASIC_STRM_URL'), 1] # 응급의료기관은 center_type == 1
    call_basic_info_Strm = PythonOperator(
        task_id='call_basic_Strm_data',
        python_callable=DataLoader.call_api,
        op_args=[op_orgs],
        provide_context=True
    )

    # 기본정보 데이터 적재 태스크 
    load_basic_data_to_rds_egyt = PythonOperator(
        task_id='load_basic_info_egyt',
        python_callable=DataLoader.load_basic_data_to_rds,
        op_kwargs={'task_id':'call_basic_Egyt_data'},
        provide_context=True
    )

    # 기본정보 데이터 적재 태스크 
    load_basic_data_to_rds_strm = PythonOperator(
        task_id='load_basic_info_strm',
        python_callable=DataLoader.load_basic_data_to_rds,
        op_kwargs={'task_id':'call_basic_Strm_data'},
        provide_context=True
    )

    load_detail_info_Egyt = PythonOperator(
        task_id='load_detail_info_Egyt',
        python_callable=DataLoader.load_detail_data_to_rds,
        op_args=[Variable.get('DETAIL_EGYT_URL')],
        provide_context=True
    )

    load_detail_info_Strm = PythonOperator(
        task_id='load_detail_info_Strm',
        python_callable=DataLoader.load_detail_data_to_rds,
        op_args=[Variable.get('DETAIL_STRM_URL')],
        provide_context=True
    )
    
    count_task_rds = PythonOperator(
        task_id='count_data_in_rds',
        python_callable=count_data_in_rds,
        provide_context=True
    )

    check_task_rds = BranchPythonOperator(
        task_id='check_task_rds',
        provide_context=True,
        python_callable=check_previous_task_result
    )

    reload_detail_info = PythonOperator(
        task_id='reload_detail_data_to_rds',
        python_callable=reload_detail_data_to_rds,
        provide_context=True
    )

    end_task_rds = EmptyOperator(
        task_id='finish_data_to_rds'
    )
    
start_task >> call_basic_info_Egyt >> load_basic_data_to_rds_egyt >> load_detail_info_Egyt
start_task >> call_basic_info_Strm >> load_basic_data_to_rds_strm >> load_detail_info_Strm
[load_detail_info_Strm, load_detail_info_Egyt] >> count_task_rds
count_task_rds >> check_task_rds >> [reload_detail_info, end_task_rds]
