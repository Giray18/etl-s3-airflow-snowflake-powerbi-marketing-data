from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pandas import DataFrame
import pandas as pd
import config

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Import SQLAlchemy to set constraints on some temporary tables
import sqlalchemy

# Define constants/variables for interacting with external systems
SNOWFLAKE_CONN_ID = "snowflake_default"
url = 'https://raw.githubusercontent.com/Giray18/etl-s3-airflow-snowflake/main/event_1.csv'

# Define an SQL query for our transform step as a Python function using the SDK.
# This function converts input file to a SQL table and applies select statement
@aql.transform
def get_item_table(input_table: Table):
    # Gathered last 1000 events
    return "SELECT * FROM {{input_table}} LIMIT 1000 "

# Define a function for transforming tables to dataframes and dataframe transformations
@aql.dataframe
def transform_dataframe(df: DataFrame):
    # Renaming columns as per needed column naming conventions
    df = df.rename(columns={"event_id": "event_event_id"})
    # df = df.rename(config.columns)
    # Droping duplicates on USER_ID column to get unique user id holding column
    df = df.drop_duplicates(subset=['event_event_id'])
    # Filtering only needed columns
    df =  df[['event_event_id']]
    # Index column implementation
    df = df.assign(row_number=range(1,len(df)+1))
    return df

# Basic DAG definition
dag = DAG(
    dag_id="d_event_id_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from github repo into Snowflake, referenced by the
    # variable `event_data`. This simulated the `extract` step of the ETL pipeline.
    event_data = aql.load_file(task_id="load_events",input_file=File(url),
    # EVENT_RAW Table being created on Snowflake as a RAW ingested no relation with further transformations
    output_table=Table(
        conn_id=SNOWFLAKE_CONN_ID,
            # apply constraints to the columns of the temporary output table,
            # which is a requirement for running the '.merge' function later in the DAG.
            columns=[
                sqlalchemy.Column("event_id", sqlalchemy.String(60), primary_key=True, nullable=False, key="event_id"),
                sqlalchemy.Column(
                    "event_time",
                    sqlalchemy.String(60),
                    nullable=False,
                ),
                sqlalchemy.Column(
                    "user_id",
                    sqlalchemy.String(60),
                    nullable=False,
                ),
                sqlalchemy.Column(
                    'event_payload', sqlalchemy.String(200),
                    nullable=False,
                ),
            ],
        ),if_exists="replace",
)



# d_user table saved into snowflake
    events_data = transform_dataframe(get_item_table(event_data),output_table = Table(
        name="d_event",
        conn_id=SNOWFLAKE_CONN_ID,
    ))

    aql.cleanup()