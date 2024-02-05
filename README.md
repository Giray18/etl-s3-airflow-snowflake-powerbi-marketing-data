# Data engineering ETL activites with S3 bucket (Storage) - Airflow (ETL) - Snowflake (Data Mart (Storage) - Power BI (Visualization)
This Repo contains activities related to ETL, data mart creation and visualization. As tool S3-AIRFLOW-SNOWFLAKE and Power BI used

# Task Scenario:
A marketing and a sales team would like to delve into their retail store digital shop item view data browsed by users in order to see mostly viewed items, sales success of same items and measure digital shop platform preference of users.
Needed data will define company`s action plan to invest on mobile app or not and measure sales amount of items comparing with views of items.

Mainly, they would like to see 4 metric as below and consulted their data engineer to be able to accurately tracking those metrics;

  - What is the most used platform on item views among channels (web, mobile app etc.) in a yearly based as time period?
  - What is the yearly amount of item views on all platforms?
  - What is the most viewed item on recent year and how many times viewed? (In the latest year on dataset)
  - What are the item view amounts and view ranks as yearly distributed?

# Current Stage:
As learned by data architects of the company online channels historical usage data resides on an AWS S3 storage location.
2 files mentioned as keeping requested tracks,

  *  items.csv consisting of below fields; (Mentioned file can be found on files)
    *  adjective : categorical value defines main function of a product (nullable)
    * category :  categorical value defines product category (not nullable)
    * created_at : date time value defines when product created at sales system (e.g SAP) (not nullable)
    * ID : unique id of a product (not nullable)
    * modifier : categorical value defines sub feature of a product (nullable)
    * name : product name being created by concat of adjective + category + modifier
    * price : product price




