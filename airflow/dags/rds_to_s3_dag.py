from datetime import datetime, timedelta
from plugins.preprocessing.data_counting import DataCounter

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

def check_data_in_rds(**kwargs):
    previous_task_result = kwargs['ti'].xcom_pull(key='count_result')
    
    if previous_task_result:
        raise AirflowFailException('load to rds waiting...')

# DAG 설정
default_args = {
    'start_date': datetime(2023, 8, 24, 0, 20, 0),
    'timezone': 'Asia/Seoul',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'rds_to_s3_Dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    count_task_rds = PythonOperator(
        task_id='count_data_in_rds',
        python_callable=DataCounter().count_data_in_rds,
        provide_context=True
    )
    
    check_task_rds = PythonOperator(
        task_id='check_data_in_rds',
        python_callable=check_data_in_rds,
        provide_context=True
    )

    mysql_to_s3_basic = SqlToS3Operator(
        task_id='rds_to_s3_basic',
        query='SELECT * FROM HOSPITAL_BASIC_INFO WHERE dt = CURRENT_DATE',
        s3_bucket='de-5-1',
        s3_key='test/{{ ds_nodash }}/basic_info_{{ ds_nodash }}.csv',
        sql_conn_id='rds_conn_id',
        aws_conn_id='aws_conn_id',
        replace=True
    )

    mysql_to_s3_detail = SqlToS3Operator(
        task_id='rds_to_s3_detail',
        query='SELECT * FROM HOSPITAL_DETAIL_INFO WHERE dt = CURRENT_DATE',
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
    

count_task_rds >> check_task_rds >> [mysql_to_s3_basic, mysql_to_s3_detail] >> end_task_s3
end_task_s3 >> [delete_ex_info_basic, delete_ex_info_detail]