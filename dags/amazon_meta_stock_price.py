import json
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import yfinance as yf
import pandas as pd

def snowflake_conn():
  hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
  conn = hook.get_conn()
  return conn.cursor()

# Fetch meta amazon historical data for last 180 trading days 

@task
def extract_yfinance_data(tickers):
  raw_stock_data = yf.download(tickers, period='1y', auto_adjust=False)
  return raw_stock_data

@task
def transform_yfinace_data(raw_data:pd.DataFrame):
  last_180d_data = raw_data.tail(180)
  format_stock_data = last_180d_data.stack(level=1, future_stack=True).reset_index()
  format_stock_data.rename(columns={'Ticker': 'Symbol'}, inplace=True)
  format_stock_data['Date'] = format_stock_data['Date'].dt.strftime('%Y-%m-%d')
  return format_stock_data.to_json(orient='records')

@task
def load_data(amazon_meta_data, target_table):
  stock_data = json.loads(amazon_meta_data)
  con = snowflake_conn()
  try:
      con.execute("BEGIN;")
      con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
        symbol VARCHAR(4) NOT NULL,
        date DATE NOT NULL,
        open NUMBER(12, 4),
        close NUMBER(12, 4),
        high NUMBER(12, 4),
        low NUMBER(12, 4),
        volume BIGINT,
        PRIMARY KEY (symbol, date));""")
      con.execute(f"""DELETE FROM {target_table}""")

      for data in stock_data:
        symbol = data["Symbol"]
        date = data["Date"]
        open = data["Open"]
        high = data["High"]
        low = data["Low"]
        close = data["Close"]
        volume = data["Volume"]
        insert_sql = f"INSERT INTO {target_table} (symbol, date, open, high, low, close, volume) VALUES ('{symbol}', '{date}', {open}, {high}, {low}, {close}, {volume})"
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
  dag_id = 'AmazonMetaStockPrice',
  start_date = datetime(2024,10,2),
  catchup=False,
  tags=['ETL'],
  schedule = '30 21 * * *'
) as dag:
   
  tickers = ['META', 'AMZN']
  target_table = "raw.stock_price_amazon_meta"

  #task dependencies 
  amzn_meta_data = extract_yfinance_data(tickers)
  stock_data = transform_yfinace_data(amzn_meta_data)
  load_data(stock_data,target_table)
