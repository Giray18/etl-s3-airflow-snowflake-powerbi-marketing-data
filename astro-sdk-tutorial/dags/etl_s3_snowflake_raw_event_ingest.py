from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pandas import DataFrame
import pandas as pd
import json

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Import SQLAlchemy to set constraints on some temporary tables
import sqlalchemy

# Define constants/variables for interacting with external systems
SNOWFLAKE_CONN_ID = "snowflake_default"
S3_FILE_PATH = "https://merkle-de-interview-case-study.s3.eu-central-1.amazonaws.com/de/event.csv"



# Define a function for transforming tables to dataframes and apply dataframe transformations
@aql.dataframe
def transform_dataframe(df: DataFrame):
    # Renaming columns as per needed column naming conventions
    df = df.rename(columns={"event.payload": "event_payload"})
    # Converting json column data type to Variant on Snowflake (Stayed as JSON but with Snowflake data type)
    df["event_payload"] = df["event_payload"].map(lambda x: json.loads(x))
    # Index column implementation
    df = df.assign(guid_event_raw=range(1,len(df)+1))
    return df

# Basic DAG definition
dag = DAG(
    dag_id="raw_events_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from s3 bucket into Snowflake, referenced by the
    # variable `event_data`. This simulated the `extract` step of the ETL pipeline.
    event_data = aql.load_file(task_id="load_events",input_file=File(S3_FILE_PATH),)


    # event_raw table created and merged into snowflake (overwrite mode) table as delta loads arrive
    # target here is to have latest delta load to snowflake by daily upload
    raw_events_data = transform_dataframe((event_data),output_table = Table(
        name="event_raw",
        conn_id=SNOWFLAKE_CONN_ID,
    ))

    # Triggering next dag
    trigger_dependent_dag = TriggerDagRunOperator(
    task_id="trigger_dependent_dag",
    trigger_dag_id="d_event_table_create",
    wait_for_completion=False,
    deferrable=False,  
    )

    # Dependencies
    event_data >> raw_events_data >> trigger_dependent_dag


    # Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()