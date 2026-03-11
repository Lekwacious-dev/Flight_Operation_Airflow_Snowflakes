import sys
from pathlib import Path
from datetime import datetime, timedelta
# Dag is the main Airflow class that defines a workflow
# PythonOperator allows us to execute Python functions as tasks in our workflow
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define Airflow home directory in the container.
# T/opt/airflow is where all Dags, scripts, and data live in the Docker container.
AIRFLOW_HOME = Path("/opt/airflow") 

# Ensure the scripts folder is in Python's module search path
# This allows us to import our custom Python functions (like run_bronze_ingestion) from bronze_ingest.py
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingestion
from scripts.silver_transform import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate
from scripts.load_gold_to_snowflake import run_gold_to_snowflake

# Default arguments for the DAG. These settings apply to all tasks in the DAG unless overridden at the task level.÷
default_args = {
    'owner': 'airflow',             # Owner of the DAG  
    'retries': 0,          # Number of times to retry a failed task (0 means no retries)
    'retry_delay': timedelta(minutes=5) # Time to wait before retrying a failed task    
}

# Define the Dag using a context manager (the "with DAG" statement). This ensures that all tasks defined within this block are associated with this DAG.
with DAG(
    dag_id='flight_ops_medallion_pipe', # Unique identifier for the DAG
    default_args=default_args,           # Apply the default arguments defined above
    start_date=datetime(2026, 3, 9),    # The date and time when the DAG should start running. Airflow will not run any tasks before this date.
    schedule_interval='*/30 * * * *',   # Cron expression that defines how often the DAG should run. In this case, it runs every 30 minutes.
    catchup=False,             # If set to True, Airflow will run all past scheduled intervals from the start_date until the current date. Setting it to False means only future scheduled runs will be executed.
) as dag:
    #Define a PythonOperator for the Bronze ingestion task.
    bronze = PythonOperator(
        task_id='bronze_ingest',            # Unique task name in this DAG
        python_callable=run_bronze_ingestion,   # The Python function to execute when this task runs
        
    )
    
    silver = PythonOperator(
        task_id='silver_transform',
        python_callable=run_silver_transform
    )

    gold = PythonOperator(
        task_id='gold_aggregate',
        python_callable=run_gold_aggregate
    )

    load_to_snowflake = PythonOperator(
        task_id='load_gold_to_snowflake',
        python_callable=run_gold_to_snowflake
    )


    bronze >> silver >> gold >> load_to_snowflake  # Set the task dependency. This means the silver task will only run after the bronze task has successfully completed.