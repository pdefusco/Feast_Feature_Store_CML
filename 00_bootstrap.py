## NB: Only run this once ##

#!rm /home/cdsw/Simple_CICD_CML/models.db

import os
import time
import uuid
import json
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pyspark.sql import SparkSession

#Extracting the correct URL from hive-site.xml
tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
root = tree.getroot()

for prop in root.findall('property'):
    if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]

print("The correct Cloud Storage 2 URL is:{}".format(storage))

os.environ['STORAGE'] = storage

### Load Historical Data

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.yarn.access.hadoopFileSystems",os.environ['STORAGE'])\
    .config("spark.hadoop.yarn.resourcemanager.principal",os.environ["HADOOP_USER_NAME"])\
    .getOrCreate()

#spark.sql("drop table default.customer_interactions_cicd")

spark.sql("""CREATE TABLE IF NOT EXISTS default.olist_customers (CUSTOMER_ID STRING, 
          CUSTOMER_UNIQUE_ID STRING,
          CUSTOMER_ZIP_CODE INT,
          CUSTOMER_CITY STRING, 
          CUSTOEMR_STATE STRING
          )""")

spark.sql("""CREATE TABLE IF NOT EXISTS default.olist_geolocation (GEOLOCATION_ZIP_CODE STRING, 
          GEOLOCATION_LAT FLOAT,
          GEOLOCATION_LONG FLOAT,
          GEOLOCATION_CITY STRING, 
          GEOLOCATION_STATE STRING
          )""")

spark.sql("""CREATE TABLE IF NOT EXISTS default.olist_order_items (ORDER_ID STRING, 
          ORDER_ITEM_ID STRING,
          PRODUCT_ID INT,
          SELLER_ID STRING, 
          SHIPPING_LIMIT_DATE TIMESTAMP, 
          PRICE FLOAT,
          FREIGHT_VALUE FLOAT
          )""")

spark.sql("""CREATE TABLE IF NOT EXISTS default.olist_order_payments (ORDER_ID STRING, 
          PAYMENT_SEQUENTIAL INT,
          PAYMENT_TYPE STRING,
          PAYMENT_INSTALLMENT INT, 
          PAYMENT_VALUE FLOAT
          )""")

spark.sql("""CREATE TABLE IF NOT EXISTS default.olist_order_reviews (REVIEW_ID STRING, 
          ORDER_ID STRING,
          REVIEW_SCORE INT,
          REVIEW_COMMENT_TITLE STRING, 
          REVIEW_COMMENT_MESSAGE STRING, 
          REVIEW_CREATION_DATE  TIMESTAMP,
          REVIEW_ANSWER_TIMESTAMP TIMESTAMP
          )""")

spark.sql("""CREATE TABLE IF NOT EXISTS default.olist_orders (ORDER_ID STRING, 
          CUSTOMER_ID STRING,
          ORDER_STATUS STRING,
          ORDER_PURCHASE_TIME TIMESTAMP, 
          ORDER_APPROVED_AT TIMESTAMP,
          ORDER_DELIVERED_CUSTOMER_POSTING TIMESTAMP,
          ORDER_DELIVERED_CUSTOMER_ACTUAL TIMESTAMP,
          ORDER_ESTIMATED_DELIVERY_DATE TIMESTAMP
          )""")

spark.sql("""CREATE TABLE IF NOT EXISTS default.olist_products (PRODUCT_ID STRING, 
          PRODUCT_CATEGORY STRING,
          PRODUCT_NAME_LENGTH INT,
          PRODUCT_DESCRIPTION_LENGTH INT, 
          PRODUCT_PHOTOS_PUBLISHED INT, 
          PRODUCT_WEIGHT_GRAMS INT, 
          PRODUCT_LENGTH_CM INT,
          PRODUCT_HEIGHT_CM INT,
          PRODUCT_WIDTH_CM INT
          )""")

spark.sql("""CREATE TABLE IF NOT EXISTS default.olist_sellers (SELLER_ID STRING, 
          SELLER_ZIP_CODE INT,
          SELLER_CITY STRING,
          SELLER_STATE STRING
          )""")

spark.sql("""CREATE TABLE IF NOT EXISTS default.product_category_translation (PRODUCT_CATEGORY_POR STRING, 
          PRODUCT_CATEGORY_ENG STRING
          )""")
 
    
### Creating Spark Dataframes from Data    
olist_customers_dataset_df = spark.read.csv("Efficient_Model_Development_CML/data/olist_customers_dataset.csv", header=True, sep=',')
olist_geolocation_dataset_df = spark.read.csv("Efficient_Model_Development_CML/data/olist_geolocation_dataset.csv", header=True, sep=',')
olist_order_items_dataset_df = spark.read.csv("Efficient_Model_Development_CML/data/olist_order_items_dataset.csv", header=True, sep=',')
olist_order_payments_dataset_df = spark.read.csv("Efficient_Model_Development_CML/data/olist_order_payments_dataset.csv", header=True, sep=',')
olist_order_reviews_dataset_df = spark.read.csv("Efficient_Model_Development_CML/data/olist_order_reviews_dataset.csv", header=True, sep=',')
olist_orders_dataset_df = spark.read.csv("Efficient_Model_Development_CML/data/olist_orders_dataset.csv", header=True, sep=',')
olist_products_dataset_df = spark.read.csv("Efficient_Model_Development_CML/data/olist_products_dataset.csv", header=True, sep=',')
olist_sellers_dataset_df = spark.read.csv("Efficient_Model_Development_CML/data/olist_sellers_dataset.csv", header=True, sep=',')
product_category_name_translation_df = spark.read.csv("Efficient_Model_Development_CML/data/product_category_name_translation.csv", header=True, sep=',')

## Loading the Spark Tables from Spark Dataframes
olist_customers_dataset_df.write.insertInto("default.olist_customers", overwrite = False) 
olist_geolocation_dataset_df.write.insertInto("default.olist_geolocation", overwrite = False) 
olist_order_items_dataset_df.write.insertInto("default.olist_order_items", overwrite = False) 
olist_order_payments_dataset_df.write.insertInto("default.olist_order_payments", overwrite = False) 
olist_order_reviews_dataset_df.write.insertInto("default.olist_order_reviews", overwrite = False) 
olist_orders_dataset_df.write.insertInto("default.olist_orders", overwrite = False) 
olist_products_dataset_df.write.insertInto("default.olist_products", overwrite = False) 
olist_sellers_dataset_df.write.insertInto("default.olist_sellers", overwrite = False) 
product_category_name_translation_df.write.insertInto("default.product_category_translation", overwrite = False) 











