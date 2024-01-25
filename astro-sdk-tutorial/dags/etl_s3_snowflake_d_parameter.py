from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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



# Define a function for transforming tables to dataframes and dataframe transformations
@aql.dataframe
def transform_dataframe(df: DataFrame):
    # Renaming columns as per needed column naming conventions
    df = df.rename(columns={"event.payload": "event_payload"})
    df["event_payload"] = df["event_payload"].map(lambda x: json.loads(x))
    # Filtering only JSON data holding column
    df =  df[['event_payload']]
    df_flat = pd.json_normalize(df['event_payload'])
    # Filtering only needed columns
    df_flat =  df_flat[['parameter_name']]
    # Droping duplicates on USER_ID column to get unique user id holding column
    df_flat = df_flat.drop_duplicates(subset=['parameter_name'])
    # Index column implementation
    df_flat = df_flat.assign(guid_parameter=range(1,len(df_flat)+1))
    return df_flat

@aql.run_raw_sql
def create_table(table: Table):
    """Create the user table data which will be the target of the merge method"""
    return """
      CREATE OR REPLACE TABLE {{table}} 
      (
      parameter_name VARCHAR(100),
      guid_parameter VARCHAR(100)
    );
    """

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

    # Create the user table data which will be the target of the merge method
    def example_snowflake_partial_table_with_append():
        d_parameter = Table(name="d_parameter", temp=True, conn_id=SNOWFLAKE_CONN_ID)
        create_user_table = create_table(table=d_parameter, conn_id=SNOWFLAKE_CONN_ID)

    example_snowflake_partial_table_with_append()


# d_user table saved into snowflake
    events_data = transform_dataframe((event_data),output_table = Table(
        name="d_parameter_raw",
        conn_id=SNOWFLAKE_CONN_ID,
    ))


# Merge statement for incremental refresh (update based on key column)
    event_data_merge = aql.merge(target_table=Table(
    name = "d_parameter",
    conn_id=SNOWFLAKE_CONN_ID,),
    source_table = events_data,
        target_conflict_columns=["parameter_name"],
        columns=["parameter_name","guid_parameter"],
        if_conflicts="ignore",
    )

    aql.cleanup()