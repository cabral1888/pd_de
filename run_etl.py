from pyspark.sql import SparkSession

import os
import sys

from etl.etl_executor import EtlExecutor
from etl.preprocess_executor import PreProcessExecutor

##################################
#
## Get environment variables
#
##################################
input_data_dir = os.environ.get("DATA_DIR_1", "/home/igor/codes/pd_de/data")
output_base_dir = os.environ.get("DATALAKE_DATA_DIR_1", "/home/igor/codes/pd_de/data/datalake")
psg_database = os.environ.get("POSTGRESQL_DATABASE", "igoruchoa")
psg_username = os.environ.get("POSTGRESQL_USERNAME", "igoruchoa")
psg_password = os.environ.get("POSTGRESQL_PASSWORD", "igoruchoa")

mode = sys.argv[1]
input_filename = sys.argv[2]

##################################
#
## Creating Spark Session
#
##################################
spark = SparkSession.builder \
    .master("local") \
    .appName(input_filename) \
    .config("spark.sql.shuffle.partittions", 8) \
    .getOrCreate()

##################################
#
## Run the Spark ETL code to ingest 
## into a postgresql running database
#
##################################
postgresql_access_dict = {
    "database": psg_database,
    "username": psg_username,
    "password": psg_password
}

if mode == "datalake":
    etl_exec = EtlExecutor(sys.argv, input_data_dir)
    etl_exec.read_file_and_store_on_datalake(spark, output_base_dir)
elif mode == "etl":
    etl_exec = EtlExecutor(sys.argv, input_data_dir)
    etl_exec.read_file_and_store_on_postgresql(spark, postgresql_access_dict)
elif mode == "pivot":
    preprocess_exec = PreProcessExecutor(sys.argv, input_data_dir)
    preprocess_exec.generate_pivot_file_user_x_category_page(spark, output_base_dir)
