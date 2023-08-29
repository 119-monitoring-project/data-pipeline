from module.util.preprocessor.load import LoadHpidInfo

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# detail info에 대해 threadpool 적용
def SaveConcurrentDB(url, **kwargs):
    upstream_task_id = list(kwargs['ti'].task.upstream_task_ids)[0]

    hpids = kwargs['ti'].xcom_pull(key=upstream_task_id) 
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    
    workers = 4

    chunk_size = len(hpids) // workers
    chunks = [hpids[i:i+chunk_size] for i in range(0, len(hpids), chunk_size)]

    chunk_1 = chunks[0]
    chunk_2 = chunks[1]
    chunk_3 = chunks[2]
    chunk_4 = chunks[3]
    logging.info(chunk_1, chunk_2)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        
        futures.append(
            executor.submit(
                LoadHpidInfo.LoadDetailInfo,
                chunk_1,
                url,
                execution_date
                )
        )
        
        futures.append(
            executor.submit(
                LoadHpidInfo.LoadDetailInfo,
                chunk_2,
                url,
                execution_date
                )
        )
        
        futures.append(
            executor.submit(
                LoadHpidInfo.LoadDetailInfo,
                chunk_3,
                url,
                execution_date
                )
        )
        
        futures.append(
            executor.submit(
                LoadHpidInfo.LoadDetailInfo,
                chunk_4,
                url,
                execution_date
                )
        )
        
        for future in as_completed(futures):
            try:
                result = future.result()  # 작업의 결과를 가져옴
                print("Task completed successfully:", result)
            except Exception as e:
                print("Task failed:", e)