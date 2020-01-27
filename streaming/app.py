from pyspark.sql import SparkSession
import logging

spark = SparkSession.builder \
     .master("local") \
     .appName("Word Count") \
     .config("spark.some.config.option", "some-value") \
     .getOrCreate()

logging.info("COMECOU")

df = spark \
  .readStream \
  .format("socket") \
  .option("host", "localhost") \
  .option("port", 9999) \
  .load()

df = df.selectExpr("value")

df.writeStream \
    .format("console")\
    .outputMode("append")\
    .start()\
    .awaitTermination()
