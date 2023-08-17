from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import xmltodict
import pymysql
import requests


# DAG 설정
default_args = {
    'start_date': datetime(2023, 8, 9),
    # 'retries': 1,
    'timezone': 'Asia/Seoul',
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'emergency_room_info',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# API 호출하여 데이터 추출
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
    
    host = Variable.get('HOST')
    database = Variable.get('DATABASE')
    username = Variable.get('USERNAME')
    password = Variable.get('PASSWORD')

    try:
        # DB Connection 생성
        conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
        cursor = conn.cursor()

    except Exception as e:
        print(e)
    
    data = kwargs['ti'].xcom_pull(key=kwargs.get('task_id'))
    
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

        query = f"INSERT IGNORE INTO HOSPITAL_BASIC_INFO (hpid, phpid, duty_emcls, duty_emcls_name, duty_addr, duty_name, duty_tel1, duty_tel3, wgs_84_lon, wgs_84_lat, center_type, dt)" \
                f" VALUES ('{hpid}', '{phpid}', '{duty_emcls}', '{duty_emcls_name}', '{duty_addr}', '{duty_name}', '{duty_tel1}', '{duty_tel3}', '{wgs_84_lon}', '{wgs_84_lat}', '{center_type}', '{dt}')"
        print(query)

        hpids.append(hpid) # 리스트에 hpid 저장
        
        cursor.execute(query)
    conn.commit() 
    
    kwargs['ti'].xcom_push(key='load_hpids', value=hpids)

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
    dt = execution_date

    query = f"INSERT IGNORE INTO HOSPITAL_DETAIL_INFO (hpid, post_cdn1, post_cdn2, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, hvgc, duty_hayn, duty_hano, duty_inf, duty_map_img, duty_eryn, duty_time_1c, duty_time_2c, duty_time_3c, duty_time_4c, duty_time_5c, duty_time_6c, duty_time_7c, duty_time_8c, duty_time_1s, duty_time_2s, duty_time_3s, duty_time_4s, duty_time_5s, duty_time_6s, duty_time_7s, duty_time_8s, mkioskty25, mkioskty1, mkisokty2, mkisokty3, mkisokty4, mkisokty5, mkisokty6, mkisokty7, mkisokty8, mkisokty9, mkisokty10, mkisokty11, dgid_id_name, hpbdn, hpccuyn, hpcuyn, hperyn, hpgryn, hpicuyn, hpnicuyn, hpopyn, dt) VALUES " \
            "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')" \
        .format(hpid, post_cdn1, post_cdn2, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, hvgc, duty_hayn, duty_hano,
                duty_inf, duty_map_img, duty_eryn, duty_time_1c, duty_time_2c, duty_time_3c, duty_time_4c,
                duty_time_5c, duty_time_6c, duty_time_7c, duty_time_8c, duty_time_1s, duty_time_2s,
                duty_time_3s, duty_time_4s, duty_time_5s, duty_time_6s, duty_time_7s, duty_time_8s, mkioskty25,
                mkioskty1, mkisokty2, mkisokty3, mkisokty4, mkisokty5, mkisokty6, mkisokty7, mkisokty8,
                mkisokty9, mkisokty10, mkisokty11, dgid_id_name, hpbdn, hpccuyn, hpcuyn, hperyn, hpgryn,
                hpicuyn, hpnicuyn, hpopyn, dt)

    cursor.execute(query)

def load_detail_data_to_rds(url, **kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    hpids = kwargs['ti'].xcom_pull(key='load_hpids') # rds에 적재된 의료기관의 hpid
    
    host = Variable.get('HOST')
    database = Variable.get('DATABASE')
    username = Variable.get('USERNAME')
    password = Variable.get('PASSWORD')
    servicekey = Variable.get('SERVICEKEY')
    
    try:
        # DB Connection 생성
        conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
        cursor = conn.cursor()

    except Exception as e:
        print(e)
    
    retry_hpids = [] # http 오류가 난 API는 재호출 시도 
    for hpid in list(hpids):
        params = {'serviceKey': servicekey, 'HPID':hpid, 'pageNo' : '1', 'numOfRows' : '9999'}
        
        response = requests.get(url, params=params)
        xmlString = response.text
        jsonString = xmltodict.parse(xmlString)
        try:
            data = jsonString['response']['body']['items']['item']
            insert_hpid_info(data, cursor, execution_date)
        except:
            retry_hpids.append(hpid)
            continue
        
    conn.commit() # 1차로 적재 완료된 데이터에 대해 commit 진행

def count_data_in_rds(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    
    host = Variable.get('HOST')
    database = Variable.get('DATABASE')
    username = Variable.get('USERNAME')
    password = Variable.get('PASSWORD')
    
    try:
        # DB Connection 생성
        conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
        cursor = conn.cursor()

    except Exception as e:
        print(e)
        
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
        return 'reload_detail_data_to_rds'  # 결과가 False면 다음 태스크로 이동
    else:
        return 'finish_data_to_rds'  # 결과가 True면 다른 태스크로 이동

def reload_detail_data_to_rds(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    retry_hpids = kwargs['ti'].xcom_pull(key='retry_hpids')
    
    host = Variable.get('HOST')
    database = Variable.get('DATABASE')
    username = Variable.get('USERNAME')
    password = Variable.get('PASSWORD')
    servicekey = Variable.get('SERVICEKEY')

    try:
        # DB Connection 생성
        conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
        cursor = conn.cursor()

    except Exception as e:
        print(e)
        
    for data in list(retry_hpids):
        hpid = data[0]
        center_type = data[1]
        
        if center_type == '0':
            url = Variable.get('DETAIL_EGYT_URL')
        elif center_type == '1':
            url = Variable.get('DETAIL_STRM_URL')
        
        params = {'serviceKey': servicekey, 'HPID':hpid, 'pageNo' : '1', 'numOfRows' : '9999'}
        
        response = requests.get(url, params=params)
        xmlString = response.text
        jsonString = xmltodict.parse(xmlString)
        try:
            data = jsonString['response']['body']['items']['item']
            insert_hpid_info(data, cursor, execution_date)
        except:
            # 이 때는 진짜 슬랙 메세지 해야 함 ~
            print('슬메~')
        
    conn.commit() 

start_task = EmptyOperator(
    task_id = 'start_data_extraction',
    dag=dag
)

op_orgs = [Variable.get('BASIC_EGYT_URL'), 0] # 응급의료기관은 center_type == 0
call_basic_info_Egyt = PythonOperator(
    task_id = 'call_basic_Egyt_data',
    python_callable=call_api,
    op_args=[op_orgs],
    provide_context=True,
    dag=dag    
)

op_orgs = [Variable.get('BASIC_STRM_URL'), 1] # 응급의료기관은 center_type == 1
call_basic_info_Strm = PythonOperator(
    task_id = 'call_basic_Strm_data',
    python_callable=call_api,
    op_args=[op_orgs],
    provide_context=True,
    dag=dag
)

# 기본정보 데이터 적재 태스크 
load_basic_data_to_rds_egyt = PythonOperator(
    task_id='load_basic_info_egyt',
    python_callable=load_basic_data_to_rds,
    op_kwargs={'task_id':'call_basic_Egyt_data'},
    provide_context=True,
    dag=dag
)

# 기본정보 데이터 적재 태스크 
load_basic_data_to_rds_strm = PythonOperator(
    task_id='load_basic_info_strm',
    python_callable=load_basic_data_to_rds,
    op_kwargs={'task_id':'call_basic_Strm_data'},
    provide_context=True,
    dag=dag
)

load_detail_info_Egyt = PythonOperator(
    task_id = 'load_detail_info_Egyt',
    python_callable=load_detail_data_to_rds,
    op_args=[Variable.get('DETAIL_EGYT_URL')],
    provide_context=True,
    dag=dag
)

load_detail_info_Strm = PythonOperator(
    task_id = 'load_detail_info_Strm',
    python_callable=load_detail_data_to_rds,
    op_args=[Variable.get('DETAIL_STRM_URL')],
    provide_context=True,
    dag=dag
)

count_task_rds = PythonOperator(
    task_id = 'count_data_in_rds',
    python_callable=count_data_in_rds,
    provide_context=True,
    dag=dag
)

check_task_rds = BranchPythonOperator(
    task_id='check_task_rds',
    provide_context=True,
    python_callable=check_previous_task_result,
    dag=dag,
)

reload_detail_info = PythonOperator(
    task_id = 'reload_detail_data_to_rds',
    python_callable=reload_detail_data_to_rds,
    provide_context=True,
    dag=dag
)

end_task_rds = EmptyOperator(
    task_id = 'finish_data_to_rds',
    dag=dag
)

mysql_to_s3_basic = SqlToS3Operator(
    task_id='rds_to_s3_basic',
    query='SELECT * FROM HOSPITAL_BASIC_INFO',
    s3_bucket='de-5-1',
    s3_key='test/{{ ds_nodash }}/basic_info_{{ ds_nodash }}.csv',
    sql_conn_id='rds_conn_id',
    aws_conn_id='aws_conn_id',
    replace=True,
    dag=dag,
)

mysql_to_s3_detail = SqlToS3Operator(
    task_id='rds_to_s3_detail',
    query='SELECT * FROM HOSPITAL_DETAIL_INFO',
    s3_bucket='de-5-1',
    s3_key='test/{{ ds_nodash }}/detail_info_{{ ds_nodash }}.csv',
    sql_conn_id='rds_conn_id',
    aws_conn_id='aws_conn_id',
    replace=True,
    dag=dag,
)

end_task_s3 = EmptyOperator(
    task_id = 'finish_data_to_s3',
    dag=dag
)

delete_ex_info_basic = MySqlOperator(
    task_id='delete_basic_ex_info',
    sql='DELETE FROM HOSPITAL_BASIC_INFO WHERE dt = "{{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}"',
    mysql_conn_id='rds_conn_id',
    autocommit=True,
    dag=dag,
)

delete_ex_info_detail = MySqlOperator(
    task_id='delete_detail_ex_info',
    sql='DELETE FROM HOSPITAL_DETAIL_INFO WHERE dt = "{{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}"',
    mysql_conn_id='rds_conn_id',
    autocommit=True,
    dag=dag,
)

# 의존성 설정
start_task >> call_basic_info_Egyt >> load_basic_data_to_rds_egyt >> load_detail_info_Egyt
start_task >> call_basic_info_Strm >> load_basic_data_to_rds_strm >> load_detail_info_Strm
[load_detail_info_Strm, load_detail_info_Egyt] >> count_task_rds
count_task_rds >> check_task_rds >> [reload_detail_info, end_task_rds]
end_task_rds >> [mysql_to_s3_basic, mysql_to_s3_detail] >> end_task_s3
end_task_s3 >> [delete_ex_info_basic, delete_ex_info_detail]