from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pandas import DataFrame
import pandas as pd
import config
import json

#sqlalchemy-Snowflake JSON handling packages
from sqlalchemy import Column, Integer, JSON, String, func, select
from sqlalchemy.orm import declarative_base, DeclarativeMeta
from sqlalchemy.sql import quoted_name

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
    # Droping duplicates on USER_ID column to get unique user id holding column
    df = df.drop_duplicates(subset=['user_id'])
    # Filtering only needed columns
    df =  df[['user_id']]
    # Index column implementation
    df = df.assign(guid_user=range(1,len(df)+1))
    return df

@aql.run_raw_sql
def create_table(table: Table):
    """Create the user table data which will be the target of the merge method"""
    return """
      CREATE OR REPLACE TABLE {{table}} 
      (
      user_id VARCHAR(100),
      guid_user VARCHAR(100)
    );
    """

# Basic DAG definition
dag = DAG(
    dag_id="d_user_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from s3 bucket into Snowflake, referenced by the
    # variable `event_data`. This simulated the `extract` step of the ETL pipeline.
    event_data = aql.load_file(task_id="load_events",input_file=File(S3_FILE_PATH),)

    # Create the user table data which will be the target of the merge method
    def example_snowflake_partial_table_with_append():
        d_user = Table(name="d_user", temp=True, conn_id=SNOWFLAKE_CONN_ID)
        create_user_table = create_table(table=d_user, conn_id=SNOWFLAKE_CONN_ID)

    example_snowflake_partial_table_with_append()

    # d_user table created and merged into snowflake table as delta loads arrive
    user_data = transform_dataframe((event_data),output_table = Table(
        name="d_user_raw",
        conn_id=SNOWFLAKE_CONN_ID,
    ))

    # Merge statement for incremental refresh (update based on key column)
    user_data_merge = aql.merge(target_table=Table(
    name="d_user",
    conn_id=SNOWFLAKE_CONN_ID,),
    source_table = user_data,
        target_conflict_columns=["user_id"],
        columns=["user_id","guid_user"],
        if_conflicts="ignore",
    )


    # Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()