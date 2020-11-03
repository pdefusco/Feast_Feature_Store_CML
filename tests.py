import os
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

print(os.environ['STORAGE'])