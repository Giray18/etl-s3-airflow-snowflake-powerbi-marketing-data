from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pandas import DataFrame
import pandas as pd
import config
import uuid

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
# This function converts input file to a SQL table and selects needed columns.
@aql.transform
def get_item_table(input_table: Table):
    return "SELECT user_id FROM {{input_table}} LIMIT 1000 "

# Define a function for transforming tables to dataframes and rename columns
@aql.dataframe
def transform_dataframe(df: DataFrame):
    df = df.rename(columns={"user_id": "event_user_id"})
    # df = df.rename(config.columns)
    df = df.drop_duplicates(subset=['event_user_id'])
    # df['guid_user'] = [uuid.uuid4() for _ in range(len(df.index))]
    return df

# Basic DAG definition
dag = DAG(
    dag_id="d_user_id_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from S3 into a temporary Table, referenced by the
    # variable `items_data`. This simulated the `extract` step of the ETL pipeline.
    event_data = aql.load_file(task_id="load_events",input_file=File(url),
    # Data file needs to have a header row. The input and output table can be replaced with any
    # valid file and connection ID.
    output_table=Table(
        # name="EVENT_RAW",
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
         use_native_support=True,
            native_support_kwargs={
            "HEADER": False,
            "SKIP_HEADER" : 1,
        },
)



# d_item_table dataframe and merge it into one on already snowflake
    events_data = transform_dataframe(get_item_table(event_data),output_table = Table(
        # name="d_user",
        conn_id=SNOWFLAKE_CONN_ID,
    ))


    event_data_merge = aql.merge(target_table=Table(
        name="d_user",
        conn_id=SNOWFLAKE_CONN_ID,),
        source_table = events_data,
        target_conflict_columns=["event_user_id"],
        columns=["event_user_id"],
        if_conflicts="update",
    )

    # Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    # item_data
    aql.cleanup()