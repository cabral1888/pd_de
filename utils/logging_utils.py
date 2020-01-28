def log(spark):
	"""
		Log information into stdout.out of Spark

		Parameters
		----------

		spark: SparkSession
			The SparkSession of the application
	"""
	log4jLogger = spark.sparkContext._jvm.org.apache.log4j 
	return log4jLogger.LogManager.getLogger(__name__)
