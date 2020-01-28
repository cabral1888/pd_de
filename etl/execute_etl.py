import json
from utils.logging_utils import log

from utils.schema_utils import convert_date_using_data_ops_schema

def read_file_and_store_on_postgresql(spark_session, file, tbl_name, schema, date_ops={}):
	'''
	Test function
	'''
	try:
		rdd = spark_session \
				.sparkContext \
				.textFile(file) \
				.map(lambda x: json.loads(x)) \
				.flatMap(lambda x: x)

		df = spark_session.createDataFrame(rdd, schema)

		if date_ops:
			df = convert_date_using_data_ops_schema(df, date_ops)

		logging.info("Number of rows: "+str(df.count()))

		try:
			df \
				.write \
				.format("jdbc") \
				.option("url", "jdbc:postgresql:igoruchoa") \
			  	.option("dbtable", "public."+tbl_name) \
			  	.option("user", "igoruchoa")\
			  	.option("password", "igoruchoa") \
			  	.mode("append") \
				.save()
		except Exception as e:
			log(spark_session).error("Error on writing database... ")
			log(spark_session).error(e)
	
	except Exception as e:
		log(spark_session).error(e)