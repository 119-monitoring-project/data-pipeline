from datetime import datetime, timedelta

from module.util.preprocessor.load import LoadHpidInfo
from module.util.preprocessor.count import CountHpids
from module.util.preprocessor.check import CheckHpids
from module.util.preprocessor.thread import SaveConcurrentDB
from module.util.notifier.slack import SlackAlert

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable

failure_token = Variable.get("SLACK_FAILURE_TOKEN")

default_args = {
    'start_date': datetime(2023, 8, 24),
    'timezone': 'Asia/Seoul',
    'on_failure_callback': SlackAlert(channel='#final_project', token=failure_token).FailAlert
}

with DAG(
    'api_to_rds_Dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    start_task = EmptyOperator(
        task_id = 'start_extraction'
    )

    # egyt data 수집하는 Taskgroup
    with TaskGroup(group_id='egyt_data_extraction', dag=dag) as egyt_data_extraction :

        op_orgs = [Variable.get('BASIC_EGYT_URL'), 0] # 응급의료기관은 center_type == 0
        call_basic_info_Egyt = PythonOperator(
            task_id='call_basic_Egyt_data',
            python_callable=LoadHpidInfo.CallAPI,
            op_args=[op_orgs],
            provide_context=True
            )

        load_basic_data_to_rds_egyt = PythonOperator(
            task_id='load_basic_info_egyt',
            python_callable=LoadHpidInfo.LoadBasicInfo,
            provide_context=True
            )
        
        load_detail_info_Egyt = PythonOperator(
            task_id='load_detail_info_Egyt',
            python_callable=SaveConcurrentDB,
            op_args=[Variable.get('DETAIL_EGYT_URL')],
            provide_context=True
            )
        
        call_basic_info_Egyt >> load_basic_data_to_rds_egyt >> load_detail_info_Egyt
        
    # strm data 수집하는 Taskgroup
    with TaskGroup(group_id='strm_data_extraction', dag=dag) as strm_data_extraction :

        op_orgs = [Variable.get('BASIC_STRM_URL'), 1] # 응급의료기관은 center_type == 1
        call_basic_info_Strm = PythonOperator(
            task_id='call_basic_Strm_data',
            python_callable=LoadHpidInfo.CallAPI,
            op_args=[op_orgs],
            provide_context=True
        )

        load_basic_data_to_rds_strm = PythonOperator(
            task_id='load_basic_info_strm',
            python_callable=LoadHpidInfo.LoadBasicInfo,
            provide_context=True
        )

        load_detail_info_Strm = PythonOperator(
            task_id='load_detail_info_Strm',
            python_callable=SaveConcurrentDB,
            op_args=[Variable.get('DETAIL_STRM_URL')],
            provide_context=True
        )
        
        call_basic_info_Strm >> load_basic_data_to_rds_strm >> load_detail_info_Strm
    
    count_task_rds = PythonOperator(
        task_id='count_data_in_rds',
        python_callable=CountHpids().CountMissingHpids,
        provide_context=True
    )

    check_task_rds = BranchPythonOperator(
        task_id='check_task_rds',
        python_callable=CheckHpids.CheckMissingHpids,
        provide_context=True
    )

    reload_detail_info = PythonOperator(
        task_id='reload_detail_data_to_rds',
        python_callable=LoadHpidInfo.ReloadDetailInfo,
        provide_context=True
    )

    end_task_rds = EmptyOperator(
        task_id='finish_data_to_rds'
    )

start_task >> [egyt_data_extraction, strm_data_extraction] >> count_task_rds
count_task_rds >> check_task_rds >> [reload_detail_info, end_task_rds]
reload_detail_info >> end_task_rds