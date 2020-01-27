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