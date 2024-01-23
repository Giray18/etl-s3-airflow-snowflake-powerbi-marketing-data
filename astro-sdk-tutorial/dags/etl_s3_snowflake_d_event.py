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
S3_FILE_PATH = "https://merkle-de-interview-case-study.s3.eu-central-1.amazonaws.com/de/event.csv"


# Define a function for transforming tables to dataframes and dataframe transformations
@aql.dataframe
def transform_dataframe(df: DataFrame):
    # Droping duplicates on EVENT_ID column to get unique EVENT_ID column
    df = df.drop_duplicates(subset=['event_id'])
    # Filtering only needed columns
    df =  df[['event_id']]
    # Index column implementation
    df = df.assign(guid_event=range(1,len(df)+1))
    return df

@aql.run_raw_sql
def create_table(table: Table):
    """Create the user table data which will be the target of the merge method"""
    return """
      CREATE OR REPLACE TABLE {{table}} 
      (
      event_id VARCHAR(100),
      guid_event VARCHAR(100)
    );
    """


# Basic DAG definition
dag = DAG(
    dag_id="d_event_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from S3 bucket into Snowflake, referenced by the
    # variable `event_data`. This simulated the `extract` step of the ETL pipeline.
    event_data = aql.load_file(task_id="load_events",input_file=File(S3_FILE_PATH),)

    # Create the user table data which will be the target of the merge method
    def example_snowflake_partial_table_with_append():
        d_event = Table(name="d_event", temp=True, conn_id=SNOWFLAKE_CONN_ID)
        create_user_table = create_table(table=d_event, conn_id=SNOWFLAKE_CONN_ID)

    example_snowflake_partial_table_with_append()


# d_event table created and merged into snowflake table as delta loads arrive
    events_data = transform_dataframe((event_data),output_table = Table(
        name = "d_event_raw",
        conn_id = SNOWFLAKE_CONN_ID,
    ))

# Merge statement for incremental refresh (update based on key column)
    event_data_merge = aql.merge(target_table=Table(
    name = "d_event",
    conn_id=SNOWFLAKE_CONN_ID,),
    source_table = events_data,
        target_conflict_columns=["event_id"],
        columns=["event_id","guid_event"],
        if_conflicts="ignore",
    )

# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()