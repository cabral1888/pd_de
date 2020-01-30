from pyspark.sql.functions import *

import json

from utils.logging_utils import log
from utils.schema_utils import convert_date_using_data_ops_schema, define_date_columns_in_df, \
    change_whitespace_on_columns_by_underscore, get_schema


class EtlExecutor:
    """
        This class implements the application in charge to
        execute ETLs in general

    """
    def __init__(self, args, data_dir):
        """

        Parameters
        ----------
        args: list
            Program arguments that will be used by the appli-
            cation
        data_dir: str
            Path where resides the input data
        """
        self._args = args
        self._input_data_dir = data_dir

    def read_file_and_store_on_postgresql(self, spark_session, postgresql_access_dict):
        """
            Read a traditional JSON ([{"a":1, "b":2}, {...}]) file 
            and store it into a postgresql database 

            Parameters
            ----------

            spark_session: SparkSession
                The SparkSession of the application

            postgresql_access_dict: dict
                The dictionary with the data necessary to 
                access the postgresql. The json must contain
                {"database":..., "username":..., "password":...}
        """

        input_filename = self._args[2]
        file = self._input_data_dir + input_filename
        tbl_name = self._args[3]
        schema_json = self._args[4]
        date_ops = json.loads(self._args[5]) if len(self._args) == 6 else None

        ##################################
        #
        ## Parse schema JSON to a Spark schema
        #
        ##################################
        schema = get_schema(schema_json)
        log(spark_session).info("Schema: " + str(schema))

        try:
            rdd = spark_session \
                .sparkContext \
                .textFile(file) \
                .map(lambda x: json.loads(x)) \
                .flatMap(lambda x: x)

            df = spark_session.createDataFrame(rdd, schema)

            if date_ops:
                df = convert_date_using_data_ops_schema(df, date_ops)

            log(spark_session).info("Number of rows: " + str(df.count()))

            try:
                df \
                    .write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql:" + postgresql_access_dict["database"]) \
                    .option("dbtable", "public." + tbl_name) \
                    .option("user", postgresql_access_dict["username"]) \
                    .option("password", postgresql_access_dict["password"]) \
                    .mode("append") \
                    .save()
            except Exception as e:
                log(spark_session).error("Error on writing database... ")
                log(spark_session).error(e)

        except Exception as e:
            log(spark_session).error(e)

    def read_file_and_store_on_datalake(self, spark_session, output_base_dir):
        """
            Read a traditional JSON ([{"a":1, "b":2}, {...}]) file 
            and store it into a postgresql database 

            Parameters
            ----------

            spark_session: SparkSession
                The SparkSession of the application

            output_base_dir: str
                The datalake location

        """
        input_filename = self._args[2]
        file = self._input_data_dir + input_filename
        output_data_name = self._args[3]
        output_path = output_base_dir + "/" + output_data_name
        schema_json = self._args[4]
        date_ops = json.loads(self._args[5]) if len(self._args) == 6 else None

        ##################################
        #
        ## Parse schema JSON to a Spark schema
        #
        ##################################
        schema = get_schema(schema_json)
        log(spark_session).info("Schema: " + str(schema))

        try:
            df = spark_session.read.schema(schema).json(file)

            log(spark_session).info("Number of rows: " + str(df.count()))

            df = convert_date_using_data_ops_schema(df, date_ops)
            df = define_date_columns_in_df(df, date_ops)
            df = change_whitespace_on_columns_by_underscore(df)

            try:
                df \
                    .write \
                    .format("parquet") \
                    .partitionBy("year", "month", "day") \
                    .mode("append") \
                    .save(output_path)

            except Exception as e:
                log(spark_session).error("Error on writing to the data lake... ")
                log(spark_session).error(e)

        except Exception as e:
            log(spark_session).error(e)
