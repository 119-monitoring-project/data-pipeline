from datetime import datetime, timedelta
from module.util.connector.redshfit import ConnectRedshift
from module.util.connector.s3 import ConnectS3
import pandas as pd
import io

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# DAG 설정
default_args = {
    'start_date': datetime(2023, 8, 28),
    'timezone': 'Asia/Seoul',
    'retry_delay': timedelta(minutes=5)
}

def ReadS3file(info_type, **kwargs):
    
    current_task_name = kwargs['task_instance'].task_id

    s3_client = ConnectS3()
    bucket_name = 'de-5-1'
    file_path = f'batch/year=2023/month=08/day=30/{info_type}_2023-08-30.csv'
    csv_temp_file = 'temp_s3_file.csv'
    s3_client.download_file(bucket_name, file_path, csv_temp_file)

    df = pd.read_csv(csv_temp_file)
    # , 제거
    if df.columns[0] != 'hpid':
        dfresult = df.drop(df.columns[0], axis=1)
    else: 
        dfresult = df
    
    # detail info의 경우, hpid, hvec, dt 컬럼만 redshift에 적재
    if info_type == 'detail':
        dfresult_bi = dfresult[['hpid', 'hvec', 'dt']]
    else:
        dfresult_bi = dfresult
    
    kwargs['ti'].xcom_push(key='bi_dataframe', value=dfresult_bi)    
    kwargs['ti'].xcom_push(key='s3_dataframe', value=dfresult)
    kwargs['ti'].xcom_push(key='file_path', value=file_path)

def LoadToS3(**kwargs):
    s3_client = ConnectS3()
    df = kwargs['ti'].xcom_pull(key='s3_dataframe')
    path = kwargs['ti'].xcom_pull(key='file_path')
    print(path)
    
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    s3_client.upload_fileobj(csv_buffer, 'de-5-1', path)
    
def LoadToReshift(info_type, **kwargs):
    dfresult_bi = kwargs['ti'].xcom_pull(key='bi_dataframe')
    
    engine, conn = ConnectRedshift()
    # DataFrame을 Redshift 테이블로 적재
    table_name = f'hospital_{info_type}_info'
    schema_name = 'public'

    # DataFrame을 Redshift에 적재
    dfresult_bi.to_sql(name=table_name, con=engine, schema=schema_name, if_exists='replace', index=False)

    conn.close()

with DAG(
    's3_daily_data_to_redshift',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
) as dag:

    start_task = EmptyOperator(
        task_id = 'start',
        dag=dag
    )

    with TaskGroup(group_id='load_basic_info', dag=dag) as load_basic_info :
        
        info_type = 'basic'
        
        read_s3_file = PythonOperator(
            task_id = 'read_s3_file_task',
            python_callable=ReadS3file,
            op_args=[info_type],
            provide_context=True,
            dag=dag
        )

        load_new_file_s3 = PythonOperator(
            task_id = 'load_to_s3_task',
            python_callable=LoadToS3,
            provide_context=True,
            dag=dag
        )

        load_bi_file_redshift = PythonOperator(
            task_id = 'load_to_redshift_task',
            python_callable=LoadToReshift,
            op_args=[info_type],
            provide_context=True,
            dag=dag
        )
        
        read_s3_file >> load_new_file_s3 >> load_bi_file_redshift

    with TaskGroup(group_id='load_detail_info', dag=dag) as load_detail_info :
        
        info_type = 'detail'

        read_s3_file = PythonOperator(
            task_id = 'read_s3_file_task',
            python_callable=ReadS3file,
            op_args=[info_type],
            provide_context=True,
            dag=dag
        )

        load_new_file_s3 = PythonOperator(
            task_id = 'load_to_s3_task',
            python_callable=LoadToS3,
            provide_context=True,
            dag=dag
        )

        load_bi_file_redshift = PythonOperator(
            task_id = 'load_to_redshift_task',
            python_callable=LoadToReshift,
            op_args=[info_type],
            provide_context=True,
            dag=dag
        )

        read_s3_file >> load_new_file_s3 >> load_bi_file_redshift

    end_task = EmptyOperator(
        task_id = 'end',
        dag=dag
    )

start_task >> [load_basic_info, load_detail_info] >> end_task