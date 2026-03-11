import pandas as pd
from time_machine import strftime
import snowflake.connector
from airflow.hooks.base import BaseHook

def run_gold_to_snowflake(**context):
    # pull the gold file path from XCom (cross-communication system in Airflow)
    gold_file = context['ti'].xcom_pull(
        key = 'gold_file',
        task_ids = 'gold_aggregate'
    )

    if not gold_file:
        raise ValueError("No gold file path found in XCom for the current execution date.")
    
    execution_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S") 

    df = pd.read_csv(gold_file) 

    conn = BaseHook.get_connection("flight_snowflake_conn") # "flight_snowflake_conn" is the connection ID defined in Airflow Connections UI

    sf_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get("account"),
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database"),
        schema=conn.extra_dejson.get("schema"),
        role=conn.extra_dejson.get("role")
    )

    merge_sql =  """
    MERGE INTO FLIGHTS.KPI.FLIGHT_KPIS AS target
    USING (
        SELECT TO_TIMESTAMP(%s) AS WINDOW_START, 
        %s AS ORIGIN_COUNTRY, 
        %s AS TOTAL_FLIGHTS, 
        %s AS AVG_VELOCITY, 
        %s AS AVG_ALTITUDE
        ) src
    ON target.WINDOW_START = src.WINDOW_START
      AND target.ORIGIN_COUNTRY = src.ORIGIN_COUNTRY
    WHEN MATCHED THEN
        UPDATE SET TOTAL_FLIGHTS = src.TOTAL_FLIGHTS, 
        AVG_VELOCITY = src.AVG_VELOCITY, 
        AVG_ALTITUDE = src.AVG_ALTITUDE,
        LOAD_TIME = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY,  AVG_ALTITUDE) 
        VALUES 
        (src.WINDOW_START, src.ORIGIN_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_VELOCITY, src.AVG_ALTITUDE);
    """

    with sf_conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(merge_sql, (
                execution_date,
                row['origin_country'],
                row['total_flights'],
                row['avg_velocity'],
                row['avg_altitude']
            ))
    sf_conn.commit()
    sf_conn.close()

    
