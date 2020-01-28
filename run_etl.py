from pyspark.sql import SparkSession

import logging
import os
import json
import sys

from etl.execute_etl import read_file_and_store_on_postgresql
from utils.schema_utils import get_schema

data_dir = os.environ.get("STREAMING_DATA_DIR_1", "/home/igor/codes/pd_de/data/datalake")

filename = sys.argv[1]
table_name = sys.argv[2]
schema_json = sys.argv[3]
date_dict = json.loads(sys.argv[4]) if len(sys.argv) == 5 else None

file = data_dir+filename

spark = SparkSession.builder \
     .master("local") \
     .appName(filename) \
     .getOrCreate()

schema = get_schema(schema_json)
logging.info("Schema course: "+str(schema))

read_file_and_store_on_postgresql(spark, file, table_name,  schema, date_dict)
