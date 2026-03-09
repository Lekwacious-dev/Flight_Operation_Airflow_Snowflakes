import pandas as pd
from pathlib import Path

# Function that creates a gold aggregate file from the silver transformed data. This function will be executed by an Airflow PythonOperator in the DAG.
def run_gold_aggregate(**context):

    # pull the silver file path from XCom (cross-communication system in Airflow)
    # Here we pull the value stored with key="silver_file" from the task silver_transform. This value should be the file path to the Silver CSV file created in the previous step of the DAG.
    silver_file = context['ti'].xcom_pull(
        key='silver_file', 
        task_ids='silver_transform'
    )

    # Safety check: If no Silver file path is found in XCom for the current execution date, we raise a ValueError to stop execution and alert us to the issue. This prevents downstream errors when trying to read a non-existent file.
    if not silver_file:
        raise ValueError("No silver file path found in xcom for the current execution date.")
    
    # Load the Silver dataset into a pandas dataframe using the file path retrieved from XCom. This allows us to perform data transformations and aggregations on the Silver data.
    df = pd.read_csv(silver_file)

    ''' 
    Create an aggregate DataFrame that groups the data by "origin_country" and calculates:
    - total_flights: the count of unique "icao24" values (representing individual flights)
    - avg_velocity: the average velocity of flights from each origin country
    - avg_altitude: the average on_ground value (which indicates whether the flight is on the ground or not) for each origin country. This is a simple way to get a sense of how
    '''
    agg = (
        df.groupby("origin_country")
        .agg(
            total_flights=pd.NamedAgg(column="icao24", aggfunc="count"),
            avg_velocity=pd.NamedAgg(column="velocity", aggfunc="mean"),
            avg_altitude=pd.NamedAgg(column="on_ground", aggfunc="sum")
        )
        # Convert the index back to normal columns after grouping and aggregation
        .reset_index()
    )

    #Create the output file path for the Gold dataset by replacing "silver" with "gold" in the Silver file path. This ensures that the Gold file is stored in the correct directory and follows a consistent naming convention.
    gold_path = str(silver_file).replace("silver", "gold")
    gold_path = Path(gold_path)

    # Store the GOLD file path in XCom
    # This allows downstram Airflow tasks to access the location of the Gold file for further processing, reporting, or downstream dependencies.
    context["ti"].xcom_push(
        key="gold_file",
        value=str(gold_path)
    )

    # Save the aggrefated GOLD as CSV file.
    agg.to_csv(gold_path, index=False)
