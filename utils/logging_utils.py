def log(spark):
	log4jLogger = spark.sparkContext._jvm.org.apache.log4j 
	return log4jLogger.LogManager.getLogger(__name__)
