import json
import pandas as pd
from pathlib import Path

# This function will be executed by an Airflow PythonOperator
# **context contains Airflow runtime metadata (task instance, execution date, etc.)
def run_silver_transform(**context):
    # Get the execution date in YYYYMMDD format for naming the output file
    execution_date = context['ds_nodash']

    # pull the bronze file path from XCom (cross-communication system in Airflow)
    # this was pushed by the bronze_ingest task in the previous step of the DAG
    bronze_file = context['ti'].xcom_pull(
        key='bronze_file', 
        task_ids='bronze_ingest'
    )

    # if no Bronze is found, stop execution and raise an error
    if not bronze_file:
        raise ValueError("No bronze file path found in XCom for the current execution date.")

    # Define the output directory for the Silver layer and ensure it exists
    silver_path = Path(f"/opt/airflow/data/silver/")
    # create the silver directory if it doesn't exist
    silver_path.mkdir(parents=True, exist_ok=True)

    # open the Bronze JSON file and load it into a Python dictionary
    with open(bronze_file, "r") as f:
        data = json.load(f)

    # Assign readable column names to the raw DataFrame based on the OpenSky API documentation
    df_raw = pd.DataFrame(data["states"])
    df_raw.columns = [
        "icao24", "callsign", "origin_country", "time_position",
        "last_contact", "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk", "spi", "position_source"
    ]

    # select only the columns needed for the Silver layer (for simplicity, we are only keeping a few columns here)
    df = df_raw[
        [
            "icao24",
            "origin_country",
            "velocity",
            "on_ground"
        ]

    ]

    # Define the output file name for the Silver layer using the execution date for uniqueness
    output_file = silver_path / f"flights_silver_{execution_date}.csv"
    # save the transformed DataFrame to a CSV file in the Silver layer directory
    df.to_csv(output_file, index=False)

    # push the Silver file path to XCom so that downstream tasks (like Gold transformations or reporting) can access it
    context["ti"].xcom_push(
        key="silver_file",      # Identifier for the stored value
        value=str(output_file)  # Store the file path as a string for easy retrieval in downstream tasks
    )

    # print(f"Silver transformation comp÷lete. Output file: {output_file}")