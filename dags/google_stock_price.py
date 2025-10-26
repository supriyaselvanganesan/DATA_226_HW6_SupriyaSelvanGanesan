from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

def snowflake_conn():
  hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
  conn = hook.get_conn()
  return conn.cursor()

@task
def extract(url):
  raw_stock_price = requests.get(url)
  return raw_stock_price.json()

@task
def last_90d_price(data):
  stock_info = []
  date_sorted = sorted(data["Time Series (Daily)"].keys(), reverse=True)
  # Getting last 90 trading days
  last_90d = date_sorted[:90]
  for date in last_90d:
    res = data["Time Series (Daily)"][date]
    res["date"] = date
    stock_info.append(res)
  return stock_info

@task
def load(records, target_table):
  con = snowflake_conn()
  try:
      con.execute("BEGIN;")
      con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
        symbol VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open FLOAT,
        close FLOAT,
        high FLOAT,
        low FLOAT,
        volume NUMBER,
        PRIMARY KEY (symbol, date));""")
      con.execute(f"""DELETE FROM {target_table}""")

      for r in records:
        open = r["1. open"]
        high = r["2. high"]
        low = r["3. low"]
        close = r["4. close"]
        volume = r["5. volume"]
        date = r["date"]
        insert_sql = f"INSERT INTO {target_table} (symbol, open, high, low, close, volume, date) VALUES ('{symbol}', {open}, {high}, {low}, {close}, {volume}, '{date}')"
        con.execute(insert_sql)
      con.execute("COMMIT;")
      print("Committed successfully.")
  except Exception as e:
      con.execute("ROLLBACK;")
      print(e)
      raise e
  finally:
      con.close()

with DAG(
    dag_id = 'GoogleStockPrice',
    start_date = datetime(2024,10,1),
    catchup=False,
    tags=['ETL'],
    schedule = '30 22 * * *'
) as dag:

  target_table = "raw.stock_price_google"
  vantage_api_key = Variable.get("vantage_api_key")
  symbol = "GOOG"
  url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}"

  #task dependencies 
  data = extract(url)
  last_90d_data = last_90d_price(data)
  load(last_90d_data, target_table)
