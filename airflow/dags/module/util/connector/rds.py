import pymysql

from airflow.models import Variable

# aws rds database에 연결함
def ConnectDB():
    host = Variable.get('HOST')
    database = Variable.get('DATABASE')
    username = Variable.get('USERNAME')
    password = Variable.get('PASSWORD')
    
    try:
        conn = pymysql.connect(host=host, user=username, passwd=password, db=database, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
        return conn, cursor
    except:
        print("Error connecting to the database:")
        return None, None
