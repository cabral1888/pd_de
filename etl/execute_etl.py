import json
import logging


def read_file_and_store_on_postgresql(spark_session, file, tbl_name, schema, date_ops={}):
	'''
	Test function
	'''
	rdd = spark_session \
			.sparkContext \
			.textFile(file) \
			.map(lambda x: json.loads(x)) \
			.flatMap(lambda x: x)

	df = spark_session.createDataFrame(rdd, schema)

	if date_ops:
		col_name = date_ops["date_column"]
		date_format = date_ops["date_format"]

		df = df \
			.selectExpr("to_timestamp("+col_name+", '"+date_format+"') as "+col_name+"_temp", "*") \
			.drop(col_name) \
			.withColumnRenamed(col_name+"_temp", col_name)

	logging.info("Number of rows: "+str(df.count()))

	df \
		.write \
		.format("jdbc") \
		.option("url", "jdbc:postgresql:igoruchoa") \
	  	.option("dbtable", "public."+tbl_name) \
	  	.option("user", "igoruchoa")\
	  	.option("password", "igoruchoa") \
	  	.mode("append") \
		.save()