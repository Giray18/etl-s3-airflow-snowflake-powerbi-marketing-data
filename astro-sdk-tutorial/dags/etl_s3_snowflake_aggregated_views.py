from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task, task_group
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pandas import DataFrame
import pandas as pd


# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Import SQLAlchemy to set constraints on some temporary tables
import sqlalchemy

# Define constants/variables for interacting with external systems
SNOWFLAKE_CONN_ID = "snowflake_default"

@aql.run_raw_sql
def yearly_item_view_count(table_1: Table, table_2: Table):
    """total item view numbers by year"""
    return """
      CREATE OR REPLACE VIEW yearly_item_view_count as 
      WITH FILTERED_EVENTS AS (SELECT * FROM {{table_1}} 
      WHERE event_name = 'view_item')
      SELECT YEAR(f.event_time) as YEAR, COUNT(f.event_time) as ITEM_VIEW
      FROM FILTERED_EVENTS f
      INNER JOIN {{table_2}} d ON f.event_parameter_value = d.item_id
      GROUP BY YEAR
      ORDER BY YEAR DESC
      ;
    """

@aql.run_raw_sql
def item_view_based_on_particular_year(table_1: Table, table_2: Table):
    """item view ranks and counts as breakdown by year and item"""
    return """
      CREATE OR REPLACE VIEW item_view_based_on_particular_year as 
      WITH FILTERED_EVENTS AS (SELECT * FROM {{table_1}} 
      WHERE event_name = 'view_item')
      SELECT YEAR(f.event_time) as YEAR, ITEM_NAME,
      COUNT(f.event_time) as ITEM_VIEW,
      DENSE_RANK() OVER (PARTITION BY YEAR(f.event_time) ORDER BY ITEM_VIEW DESC) as ITEM_VIEW_RANK
      FROM FILTERED_EVENTS f
      INNER JOIN {{table_2}} d ON f.event_parameter_value = d.item_id
      GROUP BY YEAR, ITEM_NAME
      ORDER BY YEAR,ITEM_VIEW_RANK
      ;
    """


@aql.run_raw_sql
def most_viewed_item_based_on_most_recent_year(table_1: Table, table_2: Table):
    """detecting most viewed item by recent year"""
    return """
      CREATE OR REPLACE VIEW most_viewed_item_based_on_most_recent_year as 
      WITH FILTERED_EVENTS AS (SELECT * FROM {{table_1}} 
      WHERE event_name = 'view_item')
      SELECT YEAR(f.event_time) as YEAR, ITEM_NAME,
      COUNT(f.event_time) as ITEM_VIEW,
      DENSE_RANK() OVER (PARTITION BY YEAR(f.event_time) ORDER BY ITEM_VIEW DESC) as ITEM_VIEW_RANK
      FROM FILTERED_EVENTS f
      INNER JOIN {{table_2}} d ON f.event_parameter_value = d.item_id
      WHERE YEAR = (SELECT MAX(YEAR(max_year.event_time)) FROM FILTERED_EVENTS AS max_year)
      GROUP BY YEAR, ITEM_NAME
      ORDER BY YEAR,ITEM_VIEW_RANK
      LIMIT 1
      ;
    """


@aql.run_raw_sql
def most_used_platform_in_particular_year(table_1: Table, table_2: Table):
    """target is to detect which platform mostly used on item views"""
    return """
      CREATE OR REPLACE VIEW most_used_platform_in_particular_year as 
      WITH FILTERED_EVENTS AS (SELECT * FROM {{table_1}} 
      WHERE event_name = 'view_item'),
      PLATFORM_YEAR AS(
      SELECT YEAR(f.event_time) as YEAR, EVENT_PLATFORM,
      COUNT(f.event_time) as ITEM_VIEW,
      DENSE_RANK() OVER (PARTITION BY YEAR(f.event_time) ORDER BY ITEM_VIEW DESC) as PLATFORM_USAGE
      FROM FILTERED_EVENTS f
      INNER JOIN {{table_2}} d ON f.event_parameter_value = d.item_id
      GROUP BY YEAR,EVENT_PLATFORM
      ORDER BY YEAR,PLATFORM_USAGE)
      SELECT * FROM PLATFORM_YEAR WHERE PLATFORM_USAGE = 1
      ;
    """


# Basic DAG definition
dag = DAG(
    dag_id="aggregated_views",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Creating requested KPI calculations by uploading data from snowflake to temp tables
    f_events_table = Table(name="f_events", temp=True, conn_id=SNOWFLAKE_CONN_ID)
    d_item_table = Table(name="d_item", temp=True, conn_id=SNOWFLAKE_CONN_ID)

    # KPI calculations
    year_view_count = yearly_item_view_count(f_events_table,d_item_table)
    year_item_view_count = item_view_based_on_particular_year(f_events_table,d_item_table)
    recent_year_item_most_view_count = most_viewed_item_based_on_most_recent_year(f_events_table,d_item_table)
    most_used_platform = most_used_platform_in_particular_year(f_events_table,d_item_table)

    # Dependencies between tasks
    year_view_count >> year_item_view_count >> recent_year_item_most_view_count >> most_used_platform


# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()