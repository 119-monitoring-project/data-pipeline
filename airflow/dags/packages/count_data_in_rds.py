from . import data_loader

def count_data_in_rds(**kwargs):
    # execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')

    _, cursor = data_loader.connect_db()

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