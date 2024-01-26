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
SNOWFLAKE_EVENTS_F = "f_events"
SNOWFLAKE_ITEM_D = "d_item"
# S3_FILE_PATH = "https://merkle-de-interview-case-study.s3.eu-central-1.amazonaws.com/de/event.csv"

# Define a query for aggregating data 
# @aql.transform()
# def last_five_animations(input_table: Table):  # skipcq: PYL-W0613
#     return """
#         SELECT *
#         FROM {{input_table}}
#         WHERE genre1=='Animation'
#         ORDER BY rating asc
#         LIMIT 5;
#     """


# Define a function for transforming tables to dataframes and dataframe transformations
@aql.dataframe
def transform_dataframe(df: DataFrame):
    # # Droping duplicates on EVENT_ID column to get unique EVENT_ID column
    # df = df.drop_duplicates(subset=['event_id'])
    # # Filtering only needed columns
    # df =  df[['event_id']]
    # # Index column implementation
    # df = df.assign(guid_event=range(1,len(df)+1))
    return df

@aql.transform
def filter_events(input_table: Table):
    return "SELECT * FROM {{input_table}} WHERE EVENT_NAME = 'view_item'"

@aql.transform()
def read_transform_table(table_1: Table, table_2: Table):
    """Read table data which will be the target of the merge method"""
    return """
      SELECT YEAR(f.EVENT_TIME) as YEAR, COUNT(f.EVENT_TIME) as ITEM_VIEW
      FROM {{table_1}} as f
      INNER JOIN {{table_2}} as d ON f.EVENT_PARAMETER_VALUE = d.ITEM_ID
      GROUP BY YEAR
      ORDER BY YEAR DESC
    """


# Basic DAG definition
dag = DAG(
    dag_id="year_based_count_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from S3 bucket into Snowflake, referenced by the
    # variable `event_data`. This simulated the `extract` step of the ETL pipeline.
    # event_data = aql.load_file(task_id="load_events",input_file=File(S3_FILE_PATH),)

    # Create a Table object for customer data in the Snowflake database
    f_events_table = Table(
        name = SNOWFLAKE_EVENTS_F,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    d_item_table = Table(
        name = SNOWFLAKE_ITEM_D,
        conn_id=SNOWFLAKE_CONN_ID,
    )


    year_view_count = read_transform_table(filter_events(f_events_table),d_item_table)

    year_view_count_dataframe = transform_dataframe((year_view_count),output_table = Table(
        name = "yearly_item_view_count",
        conn_id = SNOWFLAKE_CONN_ID,
    ))
                                           
                                           
    #                                        ,output_table = Table(
    #     name = "yearly_item_view_count",
    #     conn_id = SNOWFLAKE_CONN_ID,
    # ))

    # # Create the user table data which will be the target of the merge method
    # def example_snowflake_partial_table_with_append():
    #     d_event = Table(name="d_event", temp=True, conn_id=SNOWFLAKE_CONN_ID)
    #     create_user_table = create_table(table=d_event, conn_id=SNOWFLAKE_CONN_ID)

    # example_snowflake_partial_table_with_append()


# d_event table created and merged into snowflake table as delta loads arrive
#     events_data = transform_dataframe((event_data),output_table = Table(
#         name = "d_event_raw",
#         conn_id = SNOWFLAKE_CONN_ID,
#     ))

# # Merge statement for incremental refresh (update based on key column)
#     event_data_merge = aql.merge(target_table=Table(
#     name = "d_event",
#     conn_id=SNOWFLAKE_CONN_ID,),
#     source_table = events_data,
#         target_conflict_columns=["event_id"],
#         columns=["event_id","guid_event"],
#         if_conflicts="ignore",
#     )

# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()