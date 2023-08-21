import json
from datetime import datetime, timedelta
import boto3
from plugins.preprocessing.db_connecting import connect_db
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


# DAG 설정
default_args = {
    'start_date': datetime(2023, 8, 9),
    # 'retries': 1,
    'timezone': 'Asia/Seoul',
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    's3_real_time_data_to_redhsift',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)


def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_conn')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def get_s3_client():
    s3_client = boto3.client('s3',
                             aws_access_key_id=Variable.get('aws_secret_access_id'),
                             aws_secret_access_key=Variable.get('aws_secret_access_key'),
                             region_name=Variable.get('aws_region')
                             )

    return s3_client


def get_latest_file_from_s3(**context):
    bucket_name = context['params']['bucket_name']

    s3_client = get_s3_client()

    obj_list = s3_client.list_objects(Bucket=bucket_name)
    contents_list = obj_list['Contents']

    key_list = [x['Key'] for x in contents_list if 'real_time_data' in x['Key']]
    context['ti'].xcom_push(key='latest_file_name', value=sorted(key_list)[-1])


def download_file_from_s3(**context):
    file_name = context['ti'].xcom_pull(key='latest_file_name')
    bucket_name = context['params']['bucket_name']
    s3_client = get_s3_client()
    obj = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_name
    )
    latest_file = obj["Body"].read().decode('utf-8')
    context['ti'].xcom_push(key='latest_file', value=latest_file)


def insert_data_to_redshift(**context):
    now = datetime.now()
    now += timedelta(hours=9)
    latest_file = context['ti'].xcom_pull(key='latest_file')
    #cursor = get_redshift_connection()
    query = "INSERT INTO real_time_data_to_redshift (hpid, phpid, hvidate, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, " \
            "hvgc, hvdnm, hvctayn, hvmriayn, hvangioayn, hvventiayn, hvventisoayn, hvincuayn, hvcrrtayn, " \
            "hvecmoayn, hvoxyayn, hvhypoayn, hvamyn, hv1, hv2, hv3, hv4, hv5, hv6, hv7, hv8, hv9, hv10, hv11, " \
            "hv12, hv13, hv14, hv15, hv16, hv17, hv18, hv19, hv21, hv22, hv23, hv24, hv25, hv26, hv27, hv28, hv29, " \
            "hv30, hv31, hv32, hv33, hv34, hv35, hv36, hv37, hv38, hv39, hv40, hv41, hv42, hv43, dutyname, " \
            "dutytel3, hvs01, hvs02, hvs03, hvs04, hvs05, hvs06, hvs07, hvs08, hvs09, hvs10, hvs11, hvs12, hvs13, " \
            "hvs14, hvs15, hvs16, hvs17, hvs18, hvs19, hvs20, hvs21, hvs22, hvs23, hvs24, hvs25, hvs26, hvs27, " \
            "hvs28, hvs29, hvs30, hvs31, hvs32, hvs33, hvs34, hvs35, hvs36, hvs37, hvs38, hvs46, hvs47, hvs48, " \
            "hvs49, hvs50, hvs51, hvs52, hvs53, hvs54, hvs55, hvs56, hvs57, hvs58, hvs59, dt)\n" \
            "VALUES "
    json_list = latest_file.split('\n')

    for i, json_data in enumerate(json_list):
        if json_data == '':
            continue

        data = json.loads(json_data)
        if i != 0:
            query += ', '
        query += f"('{data.get('hpid', '')}', '{data.get('phpid', '')}', '{data.get('hvidate', '')}', '{data.get('hvec', '')}', '{data.get('hvoc', '')}', '{data.get('hvcc', '')}', '{data.get('hvncc', '')}', '{data.get('hvccc', '')}', '{data.get('hvicc', '')}', "\
            f"'{data.get('hvgc', '')}', '{data.get('hvdnm', '')}', '{data.get('hvctayn', '')}', '{data.get('hvmriayn', '')}', '{data.get('hvangioayn', '')}', '{data.get('hvventiayn', '')}', '{data.get('hvventisoayn', '')}', '{data.get('hvincuayn', '')}', '{data.get('hvcrrtayn', '')}', " \
            f"'{data.get('hvecmoayn', '')}', '{data.get('hvoxyayn', '')}', '{data.get('hvhypoayn', '')}', '{data.get('hvamyn', '')}', '{data.get('hv1', '')}', '{data.get('hv2', '')}', '{data.get('hv3', '')}', '{data.get('hv4', '')}', '{data.get('hv5', '')}', '{data.get('hv6', '')}', '{data.get('hv7', '')}', '{data.get('hv8', '')}', '{data.get('hv9', '')}', '{data.get('hv10', '')}', '{data.get('hv11', '')}', "\
            f"'{data.get('hv12', '')}', '{data.get('hv13', '')}', '{data.get('hv14', '')}', '{data.get('hv15', '')}', '{data.get('hv16', '')}', '{data.get('hv17', '')}', '{data.get('hv18', '')}', '{data.get('hv19', '')}', '{data.get('hv21', '')}', '{data.get('hv22', '')}', '{data.get('hv23', '')}', '{data.get('hv24', '')}', '{data.get('hv25', '')}', '{data.get('hv26', '')}', '{data.get('hv27', '')}', '{data.get('hv28', '')}', '{data.get('hv29', '')}', " \
            f"'{data.get('hv30', '')}', '{data.get('hv31', '')}', '{data.get('hv32', '')}', '{data.get('hv33', '')}', '{data.get('hv34', '')}', '{data.get('hv35', '')}', '{data.get('hv36', '')}', '{data.get('hv37', '')}', '{data.get('hv38', '')}', '{data.get('hv39', '')}', '{data.get('hv40', '')}', '{data.get('hv41', '')}', '{data.get('hv42', '')}', '{data.get('hv43', '')}', '{data.get('dutyname', '')}', " \
            f"'{data.get('dutytel3', '')}', '{data.get('hvs01', '')}', '{data.get('hvs02', '')}', '{data.get('hvs03', '')}', '{data.get('hvs04', '')}', '{data.get('hvs05', '')}', '{data.get('hvs06', '')}', '{data.get('hvs07', '')}', '{data.get('hvs08', '')}', '{data.get('hvs09', '')}', '{data.get('hvs10', '')}', '{data.get('hvs11', '')}', '{data.get('hvs12', '')}', '{data.get('hvs13', '')}', " \
            f"'{data.get('hvs14', '')}', '{data.get('hvs15', '')}', '{data.get('hvs16', '')}', '{data.get('hvs17', '')}', '{data.get('hvs18', '')}', '{data.get('hvs19', '')}', '{data.get('hvs20', '')}', '{data.get('hvs21', '')}', '{data.get('hvs22', '')}', '{data.get('hvs23', '')}', '{data.get('hvs24', '')}', '{data.get('hvs25', '')}', '{data.get('hvs26', '')}', '{data.get('hvs27', '')}', " \
            f"'{data.get('hvs28', '')}', '{data.get('hvs29', '')}', '{data.get('hvs30', '')}', '{data.get('hvs31', '')}', '{data.get('hvs32', '')}', '{data.get('hvs33', '')}', '{data.get('hvs34', '')}', '{data.get('hvs35', '')}', '{data.get('hvs36', '')}', '{data.get('hvs37', '')}', '{data.get('hvs38', '')}', '{data.get('hvs46', '')}', '{data.get('hvs47', '')}', '{data.get('hvs48', '')}', " \
            f"'{data.get('hvs49', '')}', '{data.get('hvs50', '')}', '{data.get('hvs51', '')}', '{data.get('hvs52', '')}', '{data.get('hvs53', '')}', '{data.get('hvs54', '')}', '{data.get('hvs55', '')}', '{data.get('hvs56', '')}', '{data.get('hvs57', '')}', '{data.get('hvs58', '')}', '{data.get('hvs59', '')}', '{now}')\n"
    #print(query)


def update_data_for_rds(**context):
    now = datetime.now()
    now += timedelta(hours=9)
    conn, cursor = connect_db()
    latest_file = context['ti'].xcom_pull(key='latest_file')

    delete_query = "DELETE FROM REAL_TIME_DATA"

    insert_query = "INSERT INTO REAL_TIME_DATA (hpid, phpid, hvidate, hvec, hvoc, hvcc, hvncc, hvccc, hvicc, " \
            "hvgc, hvdnm, hvctayn, hvmriayn, hvangioayn, hvventiayn, hvventisoayn, hvincuayn, hvcrrtayn, " \
            "hvecmoayn, hvoxyayn, hvhypoayn, hvamyn, hv1, hv2, hv3, hv4, hv5, hv6, hv7, hv8, hv9, hv10, hv11, " \
            "hv12, hv13, hv14, hv15, hv16, hv17, hv18, hv19, hv21, hv22, hv23, hv24, hv25, hv26, hv27, hv28, hv29, " \
            "hv30, hv31, hv32, hv33, hv34, hv35, hv36, hv37, hv38, hv39, hv40, hv41, hv42, hv43, dutyname, " \
            "dutytel3, hvs01, hvs02, hvs03, hvs04, hvs05, hvs06, hvs07, hvs08, hvs09, hvs10, hvs11, hvs12, hvs13, " \
            "hvs14, hvs15, hvs16, hvs17, hvs18, hvs19, hvs20, hvs21, hvs22, hvs23, hvs24, hvs25, hvs26, hvs27, " \
            "hvs28, hvs29, hvs30, hvs31, hvs32, hvs33, hvs34, hvs35, hvs36, hvs37, hvs38, hvs46, hvs47, hvs48, " \
            "hvs49, hvs50, hvs51, hvs52, hvs53, hvs54, hvs55, hvs56, hvs57, hvs58, hvs59, dt)\n" \
            "VALUES "
    json_list = latest_file.split('\n')

    for i, json_data in enumerate(json_list):
        if json_data == '':
            continue

        data = json.loads(json_data)
        if i != 0:
            insert_query += ', '
        insert_query += f"('{data.get('hpid', '')}', '{data.get('phpid', '')}', '{data.get('hvidate', '')}', '{data.get('hvec', '')}', '{data.get('hvoc', '')}', '{data.get('hvcc', '')}', '{data.get('hvncc', '')}', '{data.get('hvccc', '')}', '{data.get('hvicc', '')}', " \
                 f"'{data.get('hvgc', '')}', '{data.get('hvdnm', '')}', '{data.get('hvctayn', '')}', '{data.get('hvmriayn', '')}', '{data.get('hvangioayn', '')}', '{data.get('hvventiayn', '')}', '{data.get('hvventisoayn', '')}', '{data.get('hvincuayn', '')}', '{data.get('hvcrrtayn', '')}', " \
                 f"'{data.get('hvecmoayn', '')}', '{data.get('hvoxyayn', '')}', '{data.get('hvhypoayn', '')}', '{data.get('hvamyn', '')}', '{data.get('hv1', '')}', '{data.get('hv2', '')}', '{data.get('hv3', '')}', '{data.get('hv4', '')}', '{data.get('hv5', '')}', '{data.get('hv6', '')}', '{data.get('hv7', '')}', '{data.get('hv8', '')}', '{data.get('hv9', '')}', '{data.get('hv10', '')}', '{data.get('hv11', '')}', " \
                 f"'{data.get('hv12', '')}', '{data.get('hv13', '')}', '{data.get('hv14', '')}', '{data.get('hv15', '')}', '{data.get('hv16', '')}', '{data.get('hv17', '')}', '{data.get('hv18', '')}', '{data.get('hv19', '')}', '{data.get('hv21', '')}', '{data.get('hv22', '')}', '{data.get('hv23', '')}', '{data.get('hv24', '')}', '{data.get('hv25', '')}', '{data.get('hv26', '')}', '{data.get('hv27', '')}', '{data.get('hv28', '')}', '{data.get('hv29', '')}', " \
                 f"'{data.get('hv30', '')}', '{data.get('hv31', '')}', '{data.get('hv32', '')}', '{data.get('hv33', '')}', '{data.get('hv34', '')}', '{data.get('hv35', '')}', '{data.get('hv36', '')}', '{data.get('hv37', '')}', '{data.get('hv38', '')}', '{data.get('hv39', '')}', '{data.get('hv40', '')}', '{data.get('hv41', '')}', '{data.get('hv42', '')}', '{data.get('hv43', '')}', '{data.get('dutyname', '')}', " \
                 f"'{data.get('dutytel3', '')}', '{data.get('hvs01', '')}', '{data.get('hvs02', '')}', '{data.get('hvs03', '')}', '{data.get('hvs04', '')}', '{data.get('hvs05', '')}', '{data.get('hvs06', '')}', '{data.get('hvs07', '')}', '{data.get('hvs08', '')}', '{data.get('hvs09', '')}', '{data.get('hvs10', '')}', '{data.get('hvs11', '')}', '{data.get('hvs12', '')}', '{data.get('hvs13', '')}', " \
                 f"'{data.get('hvs14', '')}', '{data.get('hvs15', '')}', '{data.get('hvs16', '')}', '{data.get('hvs17', '')}', '{data.get('hvs18', '')}', '{data.get('hvs19', '')}', '{data.get('hvs20', '')}', '{data.get('hvs21', '')}', '{data.get('hvs22', '')}', '{data.get('hvs23', '')}', '{data.get('hvs24', '')}', '{data.get('hvs25', '')}', '{data.get('hvs26', '')}', '{data.get('hvs27', '')}', " \
                 f"'{data.get('hvs28', '')}', '{data.get('hvs29', '')}', '{data.get('hvs30', '')}', '{data.get('hvs31', '')}', '{data.get('hvs32', '')}', '{data.get('hvs33', '')}', '{data.get('hvs34', '')}', '{data.get('hvs35', '')}', '{data.get('hvs36', '')}', '{data.get('hvs37', '')}', '{data.get('hvs38', '')}', '{data.get('hvs46', '')}', '{data.get('hvs47', '')}', '{data.get('hvs48', '')}', " \
                 f"'{data.get('hvs49', '')}', '{data.get('hvs50', '')}', '{data.get('hvs51', '')}', '{data.get('hvs52', '')}', '{data.get('hvs53', '')}', '{data.get('hvs54', '')}', '{data.get('hvs55', '')}', '{data.get('hvs56', '')}', '{data.get('hvs57', '')}', '{data.get('hvs58', '')}', '{data.get('hvs59', '')}', '{now}')\n"

    try:
        cursor.execute(delete_query)
        cursor.execute(insert_query)
        conn.commit()
    except:
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


get_latest_file_name = PythonOperator(
        task_id='get_latest_file_name',
        python_callable=get_latest_file_from_s3,
        params={'bucket_name': 'de-5-1'},
        provide_context=True,
        dag=dag
    )

download_file_from_s3 = PythonOperator(
        task_id='download_file_from_s3',
        python_callable=download_file_from_s3,
        params={'bucket_name': 'de-5-1'},
        provide_context=True,
        dag=dag
    )

insert_data_to_redshift = PythonOperator(
        task_id='insert_data_to_redshift',
        python_callable=insert_data_to_redshift,
        provide_context=True,
        dag=dag
    )

update_data_for_rds = PythonOperator(
        task_id='update_data_for_rds',
        python_callable=update_data_for_rds,
        provide_context=True,
        dag=dag
    )


get_latest_file_name >> download_file_from_s3 >> insert_data_to_redshift >> update_data_for_rds
