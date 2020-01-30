from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql.functions import year, month, dayofmonth, col

import json
from collections import OrderedDict


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

    mapping = {"string": StringType, "integer": IntegerType, "double": DoubleType, "timestamp": StringType,
               "boolean": BooleanType}

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
        .selectExpr("to_timestamp(" + col_name + ", '" + date_format + "') as " + col_name + "_temp", "*") \
        .drop(col_name) \
        .withColumnRenamed(col_name + "_temp", col_name)


def define_date_columns_in_df(df, date_ops):
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
    return df \
        .withColumn("year", year(col(col_name))) \
        .withColumn("month", month(col(col_name))) \
        .withColumn("day", dayofmonth(col(col_name)))


def change_whitespace_on_columns_by_underscore(df):
    cols = df.columns

    new_df = df
    for column in cols:
        if " " in column:
            new_df = new_df.withColumnRenamed(column, column.replace(" ", "_"))

    return new_df
