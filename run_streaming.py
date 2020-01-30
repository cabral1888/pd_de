from pyspark.sql import SparkSession

import os
import sys

from streaming.streaming_executor import StreamingExecutor


##################################
#
## Get environment variables
#
##################################
output_base_dir = os.environ.get("STREAMING_DATA_DIR_1", "/home/igor/codes/pd_de/data/datalake")
checkpoint_dir = os.environ.get("CHECKPOINT_DIR", "/home/igor/codes/pd_de/checkpoint")
streaming_output_interval = os.environ.get("STREAMING_OUTPUT_INTERVAL", "1 minute")

streaming_name = sys.argv[1]

##################################
#
## Creating Spark Session
#
##################################
spark = SparkSession.builder \
     .master("local") \
     .appName(streaming_name) \
     .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)\
     .config("spark.sql.shuffle.partittions", 8) \
     .getOrCreate()

##################################
#
## Run structured streaming
#
##################################
streaming_exec = StreamingExecutor(args=sys.argv, output_base_dir=output_base_dir, streaming_output_interval=streaming_output_interval)
streaming_exec.read_data_from_socket_and_store_on_datalake(spark)
