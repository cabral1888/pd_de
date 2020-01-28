from pyspark.sql import SparkSession

import os
import json
import sys

from etl.execute_etl import read_file_and_store_on_postgresql
from utils.schema_utils import get_schema
from utils.logging_utils import log

##################################
#
## Get environment variables
#
##################################
data_dir = os.environ.get("DATA_DIR_1", "/home/igor/codes/pd_de/data/BASEA")
psg_database = os.environ.get("POSTGRESQL_DATABASE", "igoruchoa")
psg_username = os.environ.get("POSTGRESQL_USERNAME", "igoruchoa")
psg_password = os.environ.get("POSTGRESQL_PASSWORD", "igoruchoa")

filename = sys.argv[1]
table_name = sys.argv[2]
schema_json = sys.argv[3]
date_dict = json.loads(sys.argv[4]) if len(sys.argv) == 5 else None

file = data_dir+filename

##################################
#
## Creating Spark Session
#
##################################
spark = SparkSession.builder \
     .master("local") \
     .appName(filename) \
     .getOrCreate()

##################################
#
## Parse schema JSON to a Spark schema
#
##################################
schema = get_schema(schema_json)
log(spark).info("Schema: "+str(schema))

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
read_file_and_store_on_postgresql(spark, file, table_name,  schema, postgresql_access_dict, date_dict)
