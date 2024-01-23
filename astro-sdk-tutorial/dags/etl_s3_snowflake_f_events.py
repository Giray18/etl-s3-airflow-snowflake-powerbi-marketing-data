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
    df = df.rename(columns={"event_id": "event_id","event_time": "event_time","user_id" : "user_id","event.payload": "event_payload"})
    # Assigning row_number column
    df = df.assign(row_number=range(1,len(df)+1))
    df["event_payload"] = df["event_payload"].map(lambda x: json.loads(x))
    # Creating second dataframe only holding exploded JSON columns
    df_flat =pd.json_normalize(df['event_payload'])
    # Assigning rown_number column to second dataframe
    df_flat = df_flat.assign(row_number=range(1,len(df_flat)+1))
    # Merging raw dataframe and exploded json column holding dataframe into one dataframe
    result = pd.merge(df, df_flat, how="inner", on=["row_number"])
    # Sorting dataframe by latest event_time
    result = result.sort_values(by=['event_time'], ascending=False)
    # Drop unnecessary columns
    result = result.drop(['event_payload','row_number'],axis = 1)
    # Index column implementation
    result = result.assign(guid_event=range(1,len(result)+1))
    return result

# Basic DAG definition
dag = DAG(
    dag_id="f_events_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from github repo into Snowflake, referenced by the
    # variable `event_data`. This simulated the `extract` step of the ETL pipeline.
    event_data = aql.load_file(task_id="load_events",input_file=File(S3_FILE_PATH),)




# f_events table created and merged into snowflake table as delta loads arrive
    events_data = transform_dataframe((event_data),output_table = Table(
        # name="f_events",
        conn_id=SNOWFLAKE_CONN_ID,
    ))

# Merge statement for incremental refresh (update based on key column)
    events_data_merge = aql.merge(target_table=Table(
        name="f_events",
        conn_id=SNOWFLAKE_CONN_ID,),
        source_table = events_data,
        target_conflict_columns=["event_id","parameter_name","parameter_value"],
        columns=["event_id","event_time","user_id","event_name","platform"
                 ,"parameter_name","parameter_value","guid_event"],
        if_conflicts="update",
    )

# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()