from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector


def snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(schema, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = snowflake_conn()

    try:
        sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        #primary key uniquess check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            cur.execute(sql)
            result = cur.fetchone()
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
            
        # Duplicate record check    
        total_sql = f"SELECT COUNT(*) FROM {schema}.temp_{table}"
        cur.execute(total_sql)
        total_count = cur.fetchone()[0]
        distinct_sql = f"""
            SELECT COUNT(*) FROM (
              SELECT DISTINCT * FROM {schema}.temp_{table}
            )
        """
        cur.execute(distinct_sql)
        distinct_count = cur.fetchone()[0]

        duplicate_count = total_count - distinct_count
        logging.info(f"Duplicate check -> Total={total_count}, Distinct={distinct_count}, Duplicates={duplicate_count}")
        if duplicate_count > 0:
            raise Exception(
                f"Duplicate records detected: Total={total_count}, Distinct={distinct_count}, Duplicates={duplicate_count} "
            )
            
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} AS
            SELECT * FROM {schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise


with DAG(
    dag_id = 'BuildSummary',
    start_date = datetime(2024,10,22),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM raw.user_session_channel u
    JOIN raw.session_timestamp s ON u.sessionId=s.sessionId
    """

    run_ctas(schema, table, select_sql, primary_key='sessionId')
