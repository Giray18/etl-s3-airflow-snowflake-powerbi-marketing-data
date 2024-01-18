#import needed libraries
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import pyodbc
import os
from datetime import datetime
from airflow.decorators import task
from airflow import Dataset
from airflow.models import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import config
import uuid
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Import SQLAlchemy to set constraints on some temporary tables
import sqlalchemy

# Define constants/variables for interacting with external systems
url = 'https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv'
SNOWFLAKE_CONN_ID = "snowflake_default"
#get password from environmnet var
pwd = 'Dingilaz55!'
uid = 'sa'
#sql db details
driver = "{SQL Server Native Client 18.0}"
server = "DESKTOP-2QOF0ST\MSSQLSERVER02"
database = "AIRFLOW_SOURCE;"


#extract data from sql server
@task()
def extract():
    try:
        # src_conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=DESKTOP-2QOF0ST\MSSQLSERVER02;DATABASE=AIRFLOW_SOURCE;UID=etl;PWD=Dingilaz55!')
        # # src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)
        # # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        # # src_engine = create_engine(connection_url)
        # # src_conn = src_engine.connect()
        # src_cursor = src_conn.cursor()
        # # src_cursor.execute(""" select  * 
        # # from [AIRFLOW_SOURCE].[AIRFLOW_SOURCE].[event]""")
        # # execute query
        # query = """ select  t.name as table_name
        # # from sys.tables t where t.name in ('event') """
        # src_tables = src_cursor.fetchall()
        # src_tables = pd.read_sql_query(query, src_conn).to_dict()['table_name']

        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};' + 'SERVER=172.22.0.5' + ';DATABASE=' + database + ';UID=' + uid
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        src_engine = create_engine(connection_url)
        src_conn = src_engine.connect()
        # execute query
        query = """ select  t.name as table_name
        from sys.tables t where t.name in ('event') """
        src_tables = pd.read_sql_query(query, src_conn).to_dict()['table_name']

        for id in src_tables:
            table_name = src_tables[id]
            df = pd.read_sql_query(f'select * FROM {table_name}', src_conn)
            # load(df, table_name)
        
        event_Data = aql.load_file(input_file=File(df),output_table=Table(
        name="EVENT_RAW",
        conn_id=SNOWFLAKE_CONN_ID,
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

    except Exception as e:
        print("Data extract error: " + str(e))

#load data to snowflake
        

# def load(df, tbl):
#     try:
#         rows_imported = 0
#         engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/adventureworks')
#         print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
#         # save df to postgres
#         df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False, chunksize=100000)
#         rows_imported += len(df)
#         # add elapsed time to final print out
#         print("Data imported successful")
#     except Exception as e:
#         print("Data load error: " + str(e))

# try:
#     #call extract function
#     extract()
# except Exception as e:
#     print("Error while extracting data: " + str(e))
        
dag = DAG(
    dag_id="d_user_id_sql_server",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    extract()
    # Load a file with a header from S3 into a temporary Table, referenced by the
    # variable `items_data`. This simulated the `extract` step of the ETL pipeline.
#     event_data = aql.load_file(
#     task_id="load_events",
#     # Data file needs to have a header row. The input and output table can be replaced with any
#     # valid file and connection ID.
#     input_file=File(S3_FILE_PATH
#     )
#     output_table=Table(
#         name="EVENT_RAW",
#         conn_id=SNOWFLAKE_CONN_ID,
#             # apply constraints to the columns of the temporary output table,
#             # which is a requirement for running the '.merge' function later in the DAG.
#             columns=[
#                 sqlalchemy.Column("event_id", sqlalchemy.String(60), primary_key=True, nullable=False, key="event_id"),
#                 sqlalchemy.Column(
#                     "event_time",
#                     sqlalchemy.String(60),
#                     nullable=False,
#                 ),
#                 sqlalchemy.Column(
#                     "user_id",
#                     sqlalchemy.String(60),
#                     nullable=False,
#                 ),
#                 sqlalchemy.Column(
#                     'event_payload', sqlalchemy.String(200),
#                     nullable=False,
#                 ),
#             ],
#         ),if_exists="replace",
# )