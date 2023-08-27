from module.util.preprocessor.load import LoadHpidInfo

from concurrent.futures import ThreadPoolExecutor

# detail info에 대해 threadpool 적용
def SaveConcurrentDB(url, **kwargs):
    hpids = kwargs['ti'].xcom_pull(key='load_hpids') 
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    
    workers = 4

    chunk_size = len(hpids) // workers
    chunks = [hpids[i:i+chunk_size] for i in range(0, len(hpids), chunk_size)]

    chunk_1 = chunks[0]
    chunk_2 = chunks[1]
    chunk_3 = chunks[2]
    chunk_4 = chunks[3]

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.submit(
            LoadHpidInfo.LoadDetailInfo,
            chunk_1,
            url,
            execution_date
            )
        
        executor.submit(
            LoadHpidInfo.LoadDetailInfo,
            chunk_2,
            url,
            execution_date
            )
        
        executor.submit(
            LoadHpidInfo.LoadDetailInfo,
            chunk_3,
            url,
            execution_date
            )
        
        executor.submit(
            LoadHpidInfo.LoadDetailInfo,
            chunk_4,
            url,
            execution_date
            )
