from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from pandas import DataFrame

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Import SQLAlchemy to set constraints on some temporary tables
import sqlalchemy

# Define constants for interacting with external systems
S3_FILE_PATH = "https://merkle-de-interview-case-study.s3.eu-central-1.amazonaws.com/de/item.csv"
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_ITEM = "item"
# SNOWFLAKE_CUSTOMERS = "customers_table"
# SNOWFLAKE_REPORTING = "reporting_table"

my_dataset = Dataset("https://merkle-de-interview-case-study.s3.eu-central-1.amazonaws.com/de/item.csv")


# Define an SQL query for our transform step as a Python function using the SDK.
# This function filters out all rows with an amount value less than 150.
@aql.transform
def get_item_table(input_table: Table):
    return "SELECT * FROM {{input_table}} WHERE CATEGORY = 'instrument'"


# Define an SQL query for our transform step as a Python function using the SDK.
# This function joins two tables into a new table.
# @aql.transform
# def join_orders_customers(filtered_orders_table: Table, customers_table: Table):
#     return """SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type
#     FROM {{filtered_orders_table}} f JOIN {{customers_table}} c
#     ON f.customer_id = c.customer_id"""


# Define a function for transforming tables to dataframes
@aql.dataframe
def transform_dataframe(df: DataFrame):
    # purchase_dates = df.loc[:, "purchase_date"]
    # print("purchase dates:", purchase_dates)
    return df


# Basic DAG definition. Run the DAG starting January 1st, 2019 on a daily schedule.
dag = DAG(
    dag_id="items_table_create",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Load a file with a header from S3 into a temporary Table, referenced by the
    # variable `orders_data`. This simulated the `extract` step of the ETL pipeline.
    items_data = aql.load_file(
        # Data file needs to have a header row. The input and output table can be replaced with any
        # valid file and connection ID.
        # input_file = my_dataset,
        input_file=File(S3_FILE_PATH
            # path=S3_FILE_PATH + "/item.csv", conn_id=S3_CONN_ID
            # path=S3_FILE_PATH + "/item.csv"
            # my_dataset
        ),
        output_table=Table(
            # name=SNOWFLAKE_ITEM,
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

    # Create a Table object for item data in the Snowflake database
    # output_table = Table(
    #     name="Item_fil",
    #     conn_id=SNOWFLAKE_CONN_ID,
    # )

    item_data = get_item_table(items_data,output_table = Table(
        name="Item_fil",
        conn_id=SNOWFLAKE_CONN_ID,
    ))

    # item_data = transform_dataframe(get_item_table(items_data))

    # Filter the orders data and then join with the customer table,
    # saving the output into a temporary table referenced by the Table instance `joined_data`
    # joined_data = join_orders_customers(filter_orders(orders_data), customers_table)

    # Merge the joined data into the reporting table based on the order_id.
    # If there's a conflict in the customer_id or customer_name, then use the ones from
    # the joined data
    # reporting_table = aql.merge(
    #     target_table=Table(
    #         name=SNOWFLAKE_REPORTING,
    #         conn_id=SNOWFLAKE_CONN_ID,
    #     ),
    #     source_table=joined_data,
    #     target_conflict_columns=["order_id"],
    #     columns=["customer_id", "customer_name"],
    #     if_conflicts="update",
    # )

    # Transform the reporting table into a dataframe
    # purchase_dates = transform_dataframe(reporting_table)

    # Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    # both `orders_data` and `joined_data`
    aql.cleanup()