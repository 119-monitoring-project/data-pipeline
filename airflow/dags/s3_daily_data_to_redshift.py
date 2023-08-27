from datetime import datetime, timedelta
import boto3
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
    's3_daily_data_to_redshift',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_conn')
    conn = hook.get_conn()
    return conn, conn.cursor()

def get_s3_client():
    s3_client = boto3.client('s3',
                            aws_access_key_id=Variable.get('aws_secret_access_id'),
                            aws_secret_access_key=Variable.get('aws_secret_access_key'),
                            region_name=Variable.get('aws_region')
                            )

    return s3_client

def copy_redshift_table_with_csv(table_name, bucket_name, file_name):
    conn, cursor = get_redshift_connection()

    try:
        delete_query = f'delete from {table_name};'

        query = f"""
            COPY {table_name}
            FROM 's3://{bucket_name}/{file_name}'
            CREDENTIALS 'aws_access_key_id={Variable.get('aws_secret_access_id')};aws_secret_access_key={Variable.get('aws_secret_access_key')}'
            FORMAT AS CSV
            IGNOREHEADER 1;
        """
        cursor.execute(delete_query)
        cursor.execute(query)

        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def get_latest_file_from_s3(**context):
    now = datetime.now()
    now += timedelta(hours=9)
    today = str(now.year)+str(now.month).zfill(2)+str(now.day-4)

    bucket_name = context['params']['bucket_name']

    s3_client = get_s3_client()

    obj_list = s3_client.list_objects(Bucket=bucket_name)
    contents_list = obj_list['Contents']

    key_list = [x['Key'] for x in contents_list if f'test/{today}' in x['Key']]

    context['ti'].xcom_push(key='latest_file_names', value=sorted(key_list))


def update_data_to_redshift(**context):
    file_names = context['ti'].xcom_pull(key='latest_file_names')
    bucket_name = context['params']['bucket_name']
    print(file_names)
    copy_redshift_table_with_csv('HOSPITAL_BASIC_INFO', bucket_name, file_names[0])
    copy_redshift_table_with_csv('HOSPITAL_DETAIL_INFO', bucket_name, file_names[1])


get_latest_file_name = PythonOperator(
        task_id='get_latest_file_name',
        python_callable=get_latest_file_from_s3,
        params={'bucket_name': 'de-5-1'},
        provide_context=True,
        dag=dag
    )

update_data_to_redshift = PythonOperator(
        task_id='update_data_to_redshift',
        python_callable=update_data_to_redshift,
        params={'bucket_name': 'de-5-1'},
        provide_context=True,
        dag=dag
    )

get_latest_file_name >> update_data_to_redshift
