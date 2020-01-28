from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from utils.logging_utils import log

from utils.schema_utils import convert_date_using_data_ops_schema

def read_file_from_socket_and_store_on_datalake(spark, file_table_name, schema, streaming_output_interval, date_ops={}):
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
                .option("path", file_table_name)\
                .trigger(processingTime=streaming_output_interval) \
                .start()\
                .awaitTermination()
        except Exception as e:
            log(spark).error("Error on writing database... ")
            log(spark).error(e)
            
    except Exception as e:
        log(spark).error(e)