from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def snowflake_conn():
  hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
  conn = hook.get_conn()
  return conn.cursor()

@task
def create_and_load(cursor):
  try:
    cursor.execute("Begin")
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                        userId int not NULL,
                        sessionId varchar(32) primary key,
                        channel varchar(32) default 'direct' );
                   ''')
    cursor.execute(''' CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                        sessionId varchar(32) primary key,
                        ts timestamp );
                   ''')
    cursor.execute('''
                    CREATE OR REPLACE STAGE raw.blob_stage
                    url = 's3://s3-geospatial/readonly/'
                    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"' );
                   ''')
    cursor.execute('''
                    COPY INTO raw.user_session_channel
                    FROM @raw.blob_stage/user_session_channel.csv;
                   ''')
    cursor.execute('''
                    COPY INTO raw.session_timestamp
                    FROM @raw.blob_stage/session_timestamp.csv;
                   ''')
    cursor.execute("BEGIN")
    print("Table created and data loaded successfully.")
  except Exception as e:
     cursor.exceute("ROLLBACK")
     raise e

with DAG(
    dag_id='SessionToSnowflake',
    start_date=datetime(2025, 10, 22),
    catchup=False,
    schedule='30 2 * * *',      
    tags=['ETL']
) as dag:

    cursor = snowflake_conn()
    create_and_load(cursor)
