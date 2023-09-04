import psycopg2
from airflow.models import Variable
from sqlalchemy import create_engine

from airflow.providers.postgres.hooks.postgres import PostgresHook

class ConnectRedshift:
    # hook을 이용한 redshift 연결
    def ConnectRedshift_hook(autocommit=True):
        hook = PostgresHook(postgres_conn_id='aws_redshift_conn')
        conn = hook.get_conn()
        
        return conn, conn.cursor()

    # sqlalchemy를 이용한 redshift 연결
    def ConnectRedshift_engine():
        # Redshift connection info
        host = Variable.get("REDSHIFT_HOST")
        dbname = Variable.get("REDSHIFT_DBNAME")
        user = Variable.get("REDSHIFT_USER")
        password = Variable.get("REDSHIFT_PASSWORD")
        port = Variable.get("REDSHIFT_PORT")

        # Redshift connection
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

        return engine, conn