from collections import OrderedDict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import json


def get_schema(schema_json):
	items = (json
		.JSONDecoder(object_pairs_hook=OrderedDict)
	 	.decode(schema_json).items())

	mapping = {"string": StringType, "integer": IntegerType, "double": DoubleType, "timestamp": StringType}

	schema = StructType([StructField(k, mapping.get(v.lower())(), True) for (k, v) in items])
	return schema

def convert_date_using_data_ops_schema(df, date_ops):
	col_name = date_ops["date_column"]
	date_format = date_ops["date_format"]

	return df \
		.selectExpr("to_timestamp("+col_name+", '"+date_format+"') as "+col_name+"_temp", "*") \
		.drop(col_name) \
		.withColumnRenamed(col_name+"_temp", col_name)
