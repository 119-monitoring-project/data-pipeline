from airflow.providers.postgres.hooks.postgres import PostgresHook


def ConnectRedshift(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_redshift_conn')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    
    return conn, conn.cursor()