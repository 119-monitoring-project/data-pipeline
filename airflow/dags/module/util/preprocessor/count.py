from module.util.connector.rds import ConnectDB 

# basic info 중 detail table에 적재되지 않은 hpids 검색
class CountHpids:
    def __init__(self):
        self.cursor = None

    # rds 연결
    def ConnectDB(self):
        _, self.cursor = ConnectDB()

    # detail info에 없는 hpids 검색
    def GetMissingHpids(self):
        self.ConnectDB()       
        
        query = '''
            SELECT HOSPITAL_BASIC_INFO.hpid, HOSPITAL_BASIC_INFO.center_type
            FROM HOSPITAL_BASIC_INFO
            LEFT JOIN HOSPITAL_DETAIL_INFO ON HOSPITAL_BASIC_INFO.hpid = HOSPITAL_DETAIL_INFO.hpid
            WHERE HOSPITAL_DETAIL_INFO.hpid IS NULL;
        '''
        
        self.cursor.execute(query)
        retry_hpids = self.cursor.fetchall()
        return retry_hpids

    # missing hpids가 존재하면 해당 hpids를 return
    def CountMissingHpids(self, **kwargs):
        retry_hpids = self.GetMissingHpids()

        if not len(retry_hpids) == 0:
            kwargs['ti'].xcom_push(key='count_result', value=False)
            kwargs['ti'].xcom_push(key='retry_hpids', value=retry_hpids)