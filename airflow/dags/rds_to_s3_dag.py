from datetime import datetime, timedelta
from module.util.preprocessor.count import CountHpids
from module.util.preprocessor.check import CheckHpids
from module.util.notifier.slack import SlackAlert

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

failure_token = Variable.get("SLACK_FAILURE_TOKEN")

# DAG 설정
default_args = {
    'start_date': datetime(2023, 8, 24, 0, 30, 0),
    'timezone': 'Asia/Seoul',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': SlackAlert(channel='#airflow-practice', token=failure_token).FailAlert
}

with DAG(
    'rds_to_s3_Dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
) as dag:
    
    wait_ex_dag_sensor = ExternalTaskSensor(
    task_id='wait_for_load_to_rds_dag',
    external_dag_id='api_to_rds_Dag',  
    external_task_id='finish_data_to_rds',    
    mode='reschedule', 
    timeout=3600 
    )
    
    start_task = EmptyOperator(
        task_id = 'start_load_to_s3'
    )

    count_task_rds = PythonOperator(
        task_id='count_data_in_rds',
        python_callable=CountHpids().CountMissingHpids,
        provide_context=True
    )
    
    check_task_rds = PythonOperator(
        task_id='check_data_in_rds',
        python_callable=CheckHpids.CheckLoadingHpids,
        provide_context=True
    )
        
    info_types = ['basic', 'detail']
    copy_rds_tasks = []
    delete_s3_tasks = []
    
    
    for type in info_types:
        
        # s3 file path
        path = 'batch/year={{ execution_date.strftime("%Y") }}/month={{ execution_date.strftime("%m") }}/day={{ execution_date.strftime("%d") }}/'
        filename = type
        dt = '_{{ execution_date.strftime("%Y-%m-%d") }}.csv'
    
        filepath = path + filename + dt

        # 오늘 날짜의 데이터를 s3에 적재
        mysql_to_s3 = SqlToS3Operator(
            task_id=f'rds_to_s3_{type}',
            query=f'SELECT * FROM HOSPITAL_{type.upper()}_INFO WHERE dt = CURRENT_DATE',
            s3_bucket='de-5-1',
            s3_key=filepath,
            sql_conn_id='rds_conn_id',
            aws_conn_id='aws_conn_id',
            replace=True
        )
        copy_rds_tasks.append(mysql_to_s3)

        yesterday_dt = '{{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'
        
        # s3에 데이터 적재되면 rds에 이전 날짜 데이터를 삭제
        delete_ex_info = MySqlOperator(
            task_id=f'delete_{type}_ex_info',
            sql=f'DELETE FROM HOSPITAL_{type.upper()}_INFO WHERE dt = {yesterday_dt}',
            mysql_conn_id='rds_conn_id',
            autocommit=True
        )
        delete_s3_tasks.append(delete_ex_info)

    end_task_delete = EmptyOperator(
        task_id='finish_delete_in_rds'
    )

wait_ex_dag_sensor >> start_task >> count_task_rds >> check_task_rds >> copy_rds_tasks
for i in range(len(info_types)):
    copy_rds_tasks[i] >> delete_s3_tasks[i]
delete_s3_tasks >> end_task_delete
