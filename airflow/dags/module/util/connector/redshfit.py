import psycopg2
from airflow.models import Variable
from sqlalchemy import create_engine

def ConnectRedshift():
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