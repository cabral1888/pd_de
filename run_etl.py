from pyspark.sql import SparkSession

import os
import json
import sys

from etl.etl_executor import EtlExecutor
from etl.preprocess_executor import PreProcessExecutor
from utils.schema_utils import get_schema
from utils.logging_utils import log

##################################
#
## Get environment variables
#
##################################
data_dir = os.environ.get("DATA_DIR_1", "/home/igor/codes/pd_de/data")
output_base_dir = os.environ.get("DATALAKE_DATA_DIR_1", "/home/igor/codes/pd_de/data/datalake")
psg_database = os.environ.get("POSTGRESQL_DATABASE", "igoruchoa")
psg_username = os.environ.get("POSTGRESQL_USERNAME", "igoruchoa")
psg_password = os.environ.get("POSTGRESQL_PASSWORD", "igoruchoa")

mode = sys.argv[1]

file = data_dir+input_filename

##################################
#
## Creating Spark Session
#
##################################
spark = SparkSession.builder \
     .master("local") \
     .appName(input_filename) \
     .getOrCreate()

##################################
#
## Run the Spark ETL code to ingest 
## into a postgresql running database
#
##################################
postgresql_access_dict = {
	"database":psg_database, 
	"username":psg_username, 
	"password":psg_password
}

if mode == "datalake":
	etl_exec = EtlExecutor(sys.argv)
	etl_exec.read_file_and_store_on_datalake(spark, output_base_dir)
elif mode == "etl":
	etl_exec = EtlExecutor(sys.argv)
	etl_exec.read_file_and_store_on_postgresql(spark, postgresql_access_dict)
elif mode == "pivotting":
	preprocess_exec = PreProcessExecutor(sys.argv)
	preprocess_exec.generate_pivot_file_user_x_category_page(spark)
