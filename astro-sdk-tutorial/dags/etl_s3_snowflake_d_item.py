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
S3_FILE_PATH = "https://merkle-de-interview-case-study.s3.eu-central-1.amazonaws.com/de/item.csv"


# Define a function for transforming tables to dataframes and rename columns
@aql.dataframe
def transform_dataframe(df: DataFrame):
    df = df.rename(columns={"adjective": "item_adjective", "category": "item_category", 
    "created_at": "item_created_at","id" : "item_id", "modifier" : "item_modifier", "name" : 
    "item_name", "price" : "item_price"})
    # df = df.rename(config.columns)
    return df

@aql.run_raw_sql
def create_table(table: Table):
    """Create the user table data which will be the target of the merge method"""
    return """
      CREATE OR REPLACE TABLE {{table}} 
      (
      item_adjective VARCHAR(100),
      item_category VARCHAR(100),
      item_created_at DATETIME,
      item_id VARCHAR(100),
      item_modifier VARCHAR(100),
      item_name VARCHAR(100),
      item_price DECIMAL(20,2)
    );
    """

# Basic DAG definition
dag = DAG(
    dag_id="d_items_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from s3 buckey into Snowflake, referenced by the
    # variable `event_data`. This simulated the `extract` step of the ETL pipeline.
    items_data = aql.load_file(task_id="load_items",input_file=File(S3_FILE_PATH),)

    # Create the user table data which will be the target of the merge method
    def example_snowflake_partial_table_with_append():
        d_item = Table(name="d_item", temp=True, conn_id=SNOWFLAKE_CONN_ID)
        create_user_table = create_table(table=d_item, conn_id=SNOWFLAKE_CONN_ID)

    example_snowflake_partial_table_with_append()


# d_item table created and merged into snowflake table as delta loads arrive
    item_data = transform_dataframe((items_data),output_table = Table(
    name="d_item_raw",
    conn_id=SNOWFLAKE_CONN_ID,
    ))


# Merge statement for incremental refresh (update based on key column)
    item_data_merge = aql.merge(target_table=Table(
        name="d_item",
        conn_id=SNOWFLAKE_CONN_ID,),
        source_table = item_data,
        target_conflict_columns=["item_id"],
        columns=["item_adjective","item_category","item_created_at","item_modifier","item_id"
                 ,"item_name","item_price"],
        if_conflicts="update",
    )

# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()