from collections import OrderedDict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import json


def get_schema(schema_json):
	"""
		Map a JSON schema into a Spark schema. The
		types 'timestamp' will not be mapped to 'TimestampType'
		here. Instead, it will still be string, so we
		can parametrize the format

		Parameters
		----------

		schema_json: str
			The string containing the schema that will be
			converted into a Spark schema
	"""
	items = (json
		.JSONDecoder(object_pairs_hook=OrderedDict)
	 	.decode(schema_json).items())

	mapping = {"string": StringType, "integer": IntegerType, "double": DoubleType, "timestamp": StringType}

	schema = StructType([StructField(k, mapping.get(v.lower())(), True) for (k, v) in items])
	return schema

def convert_date_using_data_ops_schema(df, date_ops):
	"""
		This function perform a list of transformation
		in order to convert string date into timestamp

		Parameters
		----------

		df: Dataframe
			The Spark dataframe containing the data

		date_ops:
			The dictionary with the column to be parsed
			and the format (yyyy-MM-dd HH:mm:ss and va-
			riations)
	"""
	col_name = date_ops["date_column"]
	date_format = date_ops["date_format"]

	return df \
		.selectExpr("to_timestamp("+col_name+", '"+date_format+"') as "+col_name+"_temp", "*") \
		.drop(col_name) \
		.withColumnRenamed(col_name+"_temp", col_name)
