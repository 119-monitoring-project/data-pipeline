"""Module providingFunction printing python version."""
from datetime import datetime, timedelta
import xmltodict
import requests
from packages.data_loader import insert_hpid_info, connect_db
from packages.data_loader import DataLoader

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable

def count_data_in_rds(**kwargs):
    # execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')

    _, cursor = connect_db()

    query = ' \
        SELECT HOSPITAL_BASIC_INFO.hpid, HOSPITAL_BASIC_INFO.center_type \
        FROM HOSPITAL_BASIC_INFO \
        LEFT JOIN HOSPITAL_DETAIL_INFO ON HOSPITAL_BASIC_INFO.hpid = HOSPITAL_DETAIL_INFO.hpid \
        WHERE HOSPITAL_DETAIL_INFO.hpid IS NULL; \
        '
    cursor.execute(query)
    retry_hpids = cursor.fetchall()

    if not len(retry_hpids) == 0:
        kwargs['ti'].xcom_push(key='count_result', value=False)
        kwargs['ti'].xcom_push(key='retry_hpids', value=retry_hpids)

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
    'emergency_room_info',
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

    mysql_to_s3_basic = SqlToS3Operator(
        task_id='rds_to_s3_basic',
        query='SELECT * FROM HOSPITAL_BASIC_INFO',
        s3_bucket='de-5-1',
        s3_key='test/{{ ds_nodash }}/basic_info_{{ ds_nodash }}.csv',
        sql_conn_id='rds_conn_id',
        aws_conn_id='aws_conn_id',
        replace=True
    )

    mysql_to_s3_detail = SqlToS3Operator(
        task_id='rds_to_s3_detail',
        query='SELECT * FROM HOSPITAL_DETAIL_INFO',
        s3_bucket='de-5-1',
        s3_key='test/{{ ds_nodash }}/detail_info_{{ ds_nodash }}.csv',
        sql_conn_id='rds_conn_id',
        aws_conn_id='aws_conn_id',
        replace=True
    )

    end_task_s3 = EmptyOperator(
        task_id='finish_data_to_s3'
    )

    delete_ex_info_basic = MySqlOperator(
        task_id='delete_basic_ex_info',
        sql='DELETE FROM HOSPITAL_BASIC_INFO WHERE dt = "{{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}"',
        mysql_conn_id='rds_conn_id',
        autocommit=True
    )

    delete_ex_info_detail = MySqlOperator(
        task_id='delete_detail_ex_info',
        sql='DELETE FROM HOSPITAL_DETAIL_INFO WHERE dt = "{{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}"',
        mysql_conn_id='rds_conn_id',
        autocommit=True
    )

# 의존성 설정
start_task >> call_basic_info_Egyt >> load_basic_data_to_rds_egyt >> load_detail_info_Egyt
start_task >> call_basic_info_Strm >> load_basic_data_to_rds_strm >> load_detail_info_Strm
[load_detail_info_Strm, load_detail_info_Egyt] >> count_task_rds
count_task_rds >> check_task_rds >> [reload_detail_info, end_task_rds]
end_task_rds >> [mysql_to_s3_basic, mysql_to_s3_detail] >> end_task_s3
end_task_s3 >> [delete_ex_info_basic, delete_ex_info_detail]