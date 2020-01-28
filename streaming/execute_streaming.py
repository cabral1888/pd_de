from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

import logging

def read_file_from_socket_and_store_on_datalake(spark, file_table_name, schema, date_ops={}):

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
        col_name = date_ops["date_column"]
        date_format = date_ops["date_format"]

        df = df \
            .selectExpr("to_timestamp("+col_name+", '"+date_format+"') as "+col_name+"_temp", "*") \
            .drop(col_name) \
            .withColumnRenamed(col_name+"_temp", col_name)

    df.writeStream \
        .format("parquet")\
        .option("path", file_table_name)\
        .trigger(processingTime='1 minute') \
        .start()\
        .awaitTermination()
