import requests # requsts to make http api calls
import json # json to handle json data
from datetime import datetime
from pathlib import Path #cleaner way to handle path in python

# API endpoint from OpenSKY that returns live aircraft state data.
URL = "https://opensky-network.org/api/states/all"

# This function will be executed by an airflow PythonOperator
# ** context  allows Airflow to pass runtime metadata  (task instance, Dag info, etc.)
def run_bronze_ingestion(**context):
    response = requests.get(URL, timeout=30)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Convert the API response from JSON format into a Python dictionary
    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    # Create the output file path for the Bronze layer
    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    # Push the file path to Airflow XCom (cross-communication system)
    # This allows downstream tasks (Silver/Gold layers) to know
    # where the Bronze file was stored
    with open(path, "w") as f:
        json.dump(data, f)

    context['ti'].xcom_push(
        key='bronze_file', # Identifier for the stored value
        value=str(path)) # Store the file path as string.