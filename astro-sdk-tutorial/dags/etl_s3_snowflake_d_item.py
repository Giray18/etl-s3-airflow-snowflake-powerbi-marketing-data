from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
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
S3_FILE_PATH = "https://merkle-de-interview-case-study.s3.eu-central-1.amazonaws.com/de/item.csv"
SNOWFLAKE_CONN_ID = "snowflake_default"


# Define an SQL query for our transform step as a Python function using the SDK.
# This function converts input file to a SQL table.
@aql.transform
def get_item_table(input_table: Table):
    return "SELECT * FROM {{input_table}} "

# Define a function for transforming tables to dataframes and rename columns
@aql.dataframe
def transform_dataframe(df: DataFrame):
    df = df.rename(columns={"adjective": "item_adjective", "category": "item_category", 
    "created_at": "item_created_at","id" : "item_id", "modifier" : "item_modifier", "name" : 
    "item_name", "price" : "item_price"})
    # df = df.rename(config.columns)
    return df

# Basic DAG definition
dag = DAG(
    dag_id="items_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from S3 into a temporary Table, referenced by the
    # variable `items_data`. This simulated the `extract` step of the ETL pipeline.
    items_data = aql.load_file(
        # Data file needs to have a header row. The input and output table can be replaced with any
        # valid file and connection ID.
        input_file=File(S3_FILE_PATH
        ),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            # apply constraints to the columns of the temporary output table,
            # which is a requirement for running the '.merge' function later in the DAG.
            columns=[
                sqlalchemy.Column("adjective", sqlalchemy.String(60), primary_key=False, nullable=True),
                sqlalchemy.Column(
                    "category",
                    sqlalchemy.String(60),
                    nullable=False,
                ),
                sqlalchemy.Column(
                    "created_at",
                    sqlalchemy.String(60),
                    nullable=False,
                ),
                sqlalchemy.Column(
                    "id", sqlalchemy.Integer, nullable=False, key="id", primary_key=True
                ),
                sqlalchemy.Column(
                    "modifier", sqlalchemy.String(60), nullable=True,
                ),
                sqlalchemy.Column(
                    "name", sqlalchemy.String(60), nullable=False,
                ),
                sqlalchemy.Column(
                    "price", sqlalchemy.String(60), nullable=False,
                ),
            ],
        ),
    )


# d_item_table dataframe and merge it into one on already snowflake
    item_data = transform_dataframe(get_item_table(items_data),output_table = Table(
        conn_id=SNOWFLAKE_CONN_ID,
    ))


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
    # item_data
    aql.cleanup()