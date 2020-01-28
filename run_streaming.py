from pyspark.sql import SparkSession

import os
import json
import sys

from streaming.execute_streaming import read_file_from_socket_and_store_on_datalake
from utils.schema_utils import get_schema
from utils.datalake_utils import get_path_by_day
from utils.logging_utils import log

output_base_dir = os.environ.get("STREAMING_DATA_DIR_1", "/home/igor/codes/pd_de/data/datalake")
checkpoint_dir = os.environ.get("CHECKPOINT_DIR", "/home/igor/codes/pd_de/checkpoint")
streaming_output_interval = os.environ.get("STREAMING_OUTPUT_INTERVAL", "15 minutes")

streaming_name = sys.argv[1]
schema_json = sys.argv[2]
date_dict = json.loads(sys.argv[3]) if len(sys.argv) == 4 else None

spark = SparkSession.builder \
     .master("local") \
     .appName(streaming_name) \
     .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)\
     .getOrCreate()

schema = get_schema(schema_json)
log(spark).info("Schema: "+str(schema))

output_dir = get_path_by_day(output_base_dir, streaming_name)
read_file_from_socket_and_store_on_datalake(spark, output_dir,  schema, streaming_output_interval, date_dict)
