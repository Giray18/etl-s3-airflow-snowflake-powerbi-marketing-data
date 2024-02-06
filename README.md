# Data engineering and ETL activites with S3 bucket (Cloud Storage) - Airflow (ETL) - Snowflake (Data Mart (Storage)) - Power BI (Visualization) on Marketing Data Sample
This Repo contains activities related to ETL, data mart creation and visualization. As tool S3-AIRFLOW-SNOWFLAKE and Power BI used

# Task Scenario:
A marketing and a sales team would like to delve into their retail store digital shop item view data browsed by users in order to see mostly viewed items, sales success of same items and measure digital shop platform preference of users.
Needed data will define company`s action plan to invest on mobile app or not and measure sales amount of items comparing with views of items.

Mainly, they would like to see 4 metric as below and consulted their data engineer to be able to accurately tracking those metrics;

  - What is the most used platform on item views among channels (web, mobile app etc.) in a yearly based as time period?
  - What is the yearly amount of item views on all platforms?
  - What is the most viewed item on recent year and how many times viewed? (In the latest year on dataset)
  - What are the item view amounts and view ranks as yearly distributed?

# Current State:
As learned by data architects of the company, online channels historical usage data resides on an AWS S3 storage location.
2 files mentioned as keeping requested data with belo metadata, mentioned files are being upserted in a daily schedule,

* items.csv consisting of below fields and creates source of truth for items; (Mentioned file can be found on files)
  * adjective : categorical value defines main function of a product (nullable)
  * category :  categorical value defines product category (not nullable)
  * created_at : date time value defines when product created at sales system (e.g SAP) (not nullable)
  * ID : unique id of a product (not nullable)
  * modifier : categorical value defines sub feature of a product (nullable)
  * name : product name being created by concat of adjective + category + modifier (not nullable)
  * price : product price (not nullable)
 
* event.csv consisting of below fields and creates source of truth for items; (Mentioned file can be found on files)
  * event_id :  unique id assigned by online apps for user actions (not nullable)
  * event_time : date time value of event (not nullable)
  * user_id : unique id of user who commit event action (not nullable)
  * event.payload :  JSON structured field consisting of nested fields 
    * event_name : event identifier (e.g view_item) (not nullable)
    * platform : platform where event takes place (e.g web, android) (not nullable)
    * parameter_name :  parameter used in event (e.g item_id) (not nullable)
    * parameter value : value of parameter used in the event (int value) (not nullable)

# Target State:
Based on current stage definition, an ETL pipeline will be establish to transform raw data in 2 csv files to establish a data model consisting of fact and dim tables that can give users opportunity to have their self service BI. 
Also, materialized views will be located on Snowflake and PBI will consume them which will provide exact answers of what business requires to learn.
Below flow diagram will be implemented as solution.

![picture alt](flow-diagram-etl-flow-diagram.jpg)








