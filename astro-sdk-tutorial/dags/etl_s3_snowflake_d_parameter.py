from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pandas import DataFrame
import pandas as pd
import config
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



# Define a function for transforming tables to dataframes and dataframe transformations
@aql.dataframe
def transform_dataframe(df: DataFrame):
    # Renaming columns as per needed column naming conventions
    df = df.rename(columns={"event.payload": "event_payload"})
    df["event_payload"] = df["event_payload"].map(lambda x: json.loads(x))
    # df["event_payload"] = df["event_payload"].map(lambda x: pd.json_normalize(x))
    # # df = df.rename(config.columns)
    # # Droping duplicates on USER_ID column to get unique user id holding column
    # df = df.drop_duplicates(subset=['event_event_id'])
    # Filtering only needed columns
    df =  df[['event_payload']]
    df_flat =pd.json_normalize(df['event_payload'])
    # df["event_payload"] = df["event_payload"].map(lambda x: pd.json_normalize(x))
    # Index column implementation
    # df = df.assign(row_number=range(1,len(df)+1))
    return df_flat

# Basic DAG definition
dag = DAG(
    dag_id="d_parameter_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from github repo into Snowflake, referenced by the
    # variable `event_data`. This simulated the `extract` step of the ETL pipeline.
    event_data = aql.load_file(task_id="load_events",input_file=File(S3_FILE_PATH),)




# d_user table saved into snowflake
    events_data = transform_dataframe((event_data),output_table = Table(
        name="d_parameter",
        conn_id=SNOWFLAKE_CONN_ID,
    ))

    aql.cleanup()