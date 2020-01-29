from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

import json

from utils.logging_utils import log
from utils.schema_utils import convert_date_using_data_ops_schema
from utils.schema_utils import get_schema
from utils.datalake_utils import get_path_by_day

class StreamingExecutor:
    def __init__(self, args, output_base_dir, streaming_output_interval):
        self._args = args
        self._output_base_dir = output_base_dir
        self._streaming_output_interval = streaming_output_interval

    def read_data_from_socket_and_store_on_datalake(self, spark):
        """
            Read a traditional JSON ([{"a":1, "b":2}, {...}]) file 
            and store it into a postgresql database 

            Parameters
            ----------

            spark: SparkSession
                The SparkSession of the application

        """
        streaming_name = self._args[1]
        schema_json = self._args[2]
        date_ops = json.loads(self._args[3]) if len(self._args) == 4 else None

        output_dir = get_path_by_day(self._output_base_dir, streaming_name)

        ##################################
        #
        ## Parse schema JSON to a Spark schema
        #
        ##################################
        schema = get_schema(schema_json)
        log(spark).info("Schema: "+str(schema))

        try:
            df = spark \
              .readStream \
              .format("socket") \
              .option("host", "localhost") \
              .option("port", 9999) \
              .load()

            df = df \
                .select(from_json("value", schema).alias("json_data")) \
                .selectExpr("json_data.*")

            df.printSchema()

            if date_ops:
                df = convert_date_using_data_ops_schema(df, date_ops)

            try:
                df.writeStream \
                    .format("parquet")\
                    .option("path", output_dir)\
                    .trigger(processingTime=self._streaming_output_interval) \
                    .start()\
                    .awaitTermination()

            except Exception as e:
                log(spark).error("Error on writing database... ")
                log(spark).error(e)
                
        except Exception as e:
            log(spark).error(e)