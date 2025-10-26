from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def train(cur):
    train_input_table      = "RAW.STOCK_PRICE_AMAZON_META"
    train_view             = "ADHOC.STOCK_PRICE_TRAIN_VIEW"
    forecast_function_name = "ANALYTICS.PREDICT_STOCK_PRICE"

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS
        SELECT DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA        => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME    => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME    => 'CLOSE',
        CONFIG_OBJECT     => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print("Training failed:", e)
        raise

@task
def predict(cur):
    forecast_function_name = "ANALYTICS.PREDICT_STOCK_PRICE"
    train_input_table      = "RAW.STOCK_PRICE_AMAZON_META"
    forecast_table         = "ADHOC.STOCK_PRICE_FORECAST"
    final_table            = "ANALYTICS.AMAZON_META_DATA"

    sql = f"""
    BEGIN
        -- 1) Forecast and persist to ADHOC table
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT       => {{ 'prediction_interval': 0.95 }}
        );
        LET qid := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS
        SELECT * FROM TABLE(RESULT_SCAN(:qid));

        -- 2) Build final unioned analytics table
        CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT REPLACE(series, '\"', '') AS SYMBOL, TS::DATE AS DATE,
               NULL AS actual, FORECAST, LOWER_BOUND, UPPER_BOUND
        FROM {forecast_table};

        COMMIT;
    END;"""

    try:
        cur.execute(sql)
    except Exception as e:
        # best-effort rollback if the block started but failed
        try: cur.execute("ROLLBACK;")
        except: pass
        print("Prediction/publish failed:", e)
        raise

with DAG(
    dag_id='TrainPredictStockPrice',
    start_date=datetime(2025, 10, 6),
    catchup=False,
    schedule='30 22 * * *',      # run after your ETL
    tags=['ML', 'Forecast']
) as dag:
    cur = return_snowflake_conn()
    t1 = train(cur)
    t2 = predict(cur)
    t1 >> t2