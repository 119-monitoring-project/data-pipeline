from airflow.exceptions import AirflowFailException

class CheckHpids:
    def CheckMissingHpids(**kwargs):
        previous_task_result = kwargs['ti'].xcom_pull(key='count_result')

        if previous_task_result == False:
            return 'reload_detail_data_to_rds'  # 결과가 False면 detail 재적재
        else:
            return 'finish_data_to_rds'  # 결과가 True면 rds to s3 진행
        
    def CheckLoadingHpids(**kwargs):
        previous_task_result = kwargs['ti'].xcom_pull(key='count_result')
        
        if previous_task_result:
            raise AirflowFailException('load to rds waiting...')