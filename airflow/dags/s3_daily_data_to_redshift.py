from datetime import datetime, timedelta
from module.util.connector.redshift import ConnectRedshift
from module.util.connector.s3 import ConnectS3
import pandas as pd
import io

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# 오늘 날짜의 s3 file을 download
def ReadS3file(info_type, **kwargs):
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
    
    kwargs['ti'].xcom_push(key=f'bi_dataframe_{info_type}', value=dfresult_bi)    
    kwargs['ti'].xcom_push(key=f's3_dataframe_{info_type}', value=dfresult)
    kwargs['ti'].xcom_push(key=f'file_path_{info_type}', value=file_path)

# 전처리 진행한 후 s3에 재적재
def LoadToS3(**kwargs):
    s3_client = ConnectS3()
    df = kwargs['ti'].xcom_pull(key=f's3_dataframe_{info_type}')
    path = kwargs['ti'].xcom_pull(key=f'file_path_{info_type}')
    print(path)
    
    # .csv을 이용하지 않기 위함
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    s3_client.upload_fileobj(csv_buffer, 'de-5-1', path)

# deatil, basic 포맷에 맞게 redshift에 적재
def LoadToReshift(info_type, **kwargs):
    dfresult_bi = kwargs['ti'].xcom_pull(key=f'bi_dataframe_{info_type}')
    
    engine, conn = ConnectRedshift.ConnectRedshift_engine()
    # DataFrame을 Redshift 테이블로 적재
    table_name = f'hospital_{info_type}_info'
    schema_name = 'public'

    # DataFrame을 Redshift에 적재
    dfresult_bi.to_sql(name=table_name, con=engine, schema=schema_name, if_exists='replace', index=False)

    conn.close()

# Dag default 설정
default_args = {
    'start_date': datetime(2023, 8, 28),
    'timezone': 'Asia/Seoul',
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    's3_daily_data_to_redshift',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
) as dag:

    start_task = EmptyOperator(
        task_id = 'start',
        dag=dag
    )
    
    info_types = ['basic', 'detail']
    read_s3_tasks = []
    load_s3_tasks = []
    load_redshift_tasks = []
    
    for info_type in info_types:
        read_s3_file = PythonOperator(
            task_id = 'read_s3_file_task_' + info_type,
            python_callable=ReadS3file,
            op_args=[info_type],
            provide_context=True,
            dag=dag
        )
        read_s3_tasks.append(read_s3_file)

        load_new_file_s3 = PythonOperator(
            task_id = 'load_to_s3_task_' + info_type,
            python_callable=LoadToS3,
            provide_context=True,
            dag=dag
        )
        load_s3_tasks.append(load_new_file_s3)

        load_bi_file_redshift = PythonOperator(
            task_id = 'load_to_redshift_task_' + info_type,
            python_callable=LoadToReshift,
            op_args=[info_type],
            provide_context=True,
            dag=dag
        )
        load_redshift_tasks.append(load_bi_file_redshift)

    end_task = EmptyOperator(
        task_id = 'end',
        dag=dag
    )

start_task >> read_s3_tasks
for i in range(len(info_types)):
    read_s3_tasks[i] >> load_s3_tasks[i] >> load_redshift_tasks[i]
load_redshift_tasks >> end_task