"""Module providingFunction printing python version."""
from datetime import datetime, timedelta
import xmltodict
import requests
from module.util.data_loading import insert_hpid_info, connect_db, DataLoader
from module.util.data_counting import DataCounter
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from time import sleep

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
            insert_hpid_info(data, cursor, execution_date)
        except:
            print("slack ! ! !")
            continue

    conn.commit() 


# DAG 설정
default_args = {
    'start_date': datetime(2023, 8, 24),
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
        task_id = 'start_extraction'
    )

    with TaskGroup(group_id='egyt_data_extraction', dag=dag) as egyt_data_extraction :

        op_orgs = [Variable.get('BASIC_EGYT_URL'), 0] # 응급의료기관은 center_type == 0
        call_basic_info_Egyt = PythonOperator(
            task_id='call_basic_Egyt_data',
            python_callable=DataLoader.call_api,
            op_args=[op_orgs],
            provide_context=True
            )
        
        # 기본정보 데이터 적재 태스크 
        load_basic_data_to_rds_egyt = PythonOperator(
            task_id='load_basic_info_egyt',
            python_callable=DataLoader.load_basic_data_to_rds,
            provide_context=True
            )
        
        data_loader = DataLoader()
        load_detail_info_Egyt = PythonOperator(
            task_id='load_detail_info_Egyt',
            python_callable=DataLoader.concurrent_db_saving,
            op_args=[data_loader, Variable.get('DETAIL_EGYT_URL')],
            provide_context=True
            )
        
        call_basic_info_Egyt >> load_basic_data_to_rds_egyt >> load_detail_info_Egyt
        
        
    with TaskGroup(group_id='strm_data_extraction', dag=dag) as strm_data_extraction :

        op_orgs = [Variable.get('BASIC_STRM_URL'), 1] # 응급의료기관은 center_type == 1
        call_basic_info_Strm = PythonOperator(
            task_id='call_basic_Strm_data',
            python_callable=DataLoader.call_api,
            op_args=[op_orgs],
            provide_context=True
        )

        # 기본정보 데이터 적재 태스크 
        load_basic_data_to_rds_strm = PythonOperator(
            task_id='load_basic_info_strm',
            python_callable=DataLoader.load_basic_data_to_rds,
            provide_context=True
        )

        data_loader = DataLoader()
        load_detail_info_Strm = PythonOperator(
            task_id='load_detail_info_Strm',
            python_callable=DataLoader.concurrent_db_saving,
            op_args=[data_loader, Variable.get('DETAIL_STRM_URL')],
            provide_context=True
        )
        
        call_basic_info_Strm >> load_basic_data_to_rds_strm >> load_detail_info_Strm
    
    count_task_rds = PythonOperator(
        task_id='count_data_in_rds',
        python_callable=DataCounter().count_data_in_rds,
        provide_context=True
    )

    check_task_rds = BranchPythonOperator(
        task_id='check_task_rds',
        python_callable=check_previous_task_result,
        provide_context=True,
    )

    reload_detail_info = PythonOperator(
        task_id='reload_detail_data_to_rds',
        python_callable=reload_detail_data_to_rds,
        provide_context=True
    )

    end_task_rds = EmptyOperator(
        task_id='finish_data_to_rds'
    )


start_task >> [egyt_data_extraction, strm_data_extraction] >> count_task_rds
count_task_rds >> check_task_rds >> [reload_detail_info, end_task_rds]
reload_detail_info >> end_task_rds