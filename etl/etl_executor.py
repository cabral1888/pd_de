from pyspark.sql.functions import *

import json

from utils.logging_utils import log
from utils.schema_utils import convert_date_using_data_ops_schema, define_date_columns_in_df, change_whitespace_on_columns_by_underscore


class EtlExecutor:
    __init__(self, args):
        self._args = args

    def read_file_and_store_on_postgresql(spark_session, postgresql_access_dict):
        """
            Read a traditional JSON ([{"a":1, "b":2}, {...}]) file 
            and store it into a postgresql database 

            Parameters
            ----------

            spark_session: SparkSession
                The SparkSession of the application

            file: str
                The file path of the JSON

            tbl_name: str
                The name of the table that is going to be
                created on postgresql

            schema: StructuredType(Array(StructuredField))
                The Spark schema ready to be used to con-
                vert JSON into a Spark Dataframe

            postgresql_access_dict: dict
                The dictionary with the data necessary to 
                access the postgresql. The json must contain
                {"database":..., "username":..., "password":...}

            date_ops: dict, optional
                The dictionary with the options necessary
                to parse string date into a timestamp

        """
        file = self._args[2]
        tbl_name = self._args[3]
        schema_json = self._args[4]
        date_ops = json.loads(self._args[5]) if len(self._args) == 6 else None

        ##################################
        #
        ## Parse schema JSON to a Spark schema
        #
        ##################################
        schema = get_schema(schema_json)
        log(spark).info("Schema: "+str(schema))

        try:
            rdd = spark_session \
                    .sparkContext \
                    .textFile(file) \
                    .map(lambda x: json.loads(x)) \
                    .flatMap(lambda x: x)

            df = spark_session.createDataFrame(rdd, schema)

            if date_ops:
                df = convert_date_using_data_ops_schema(df, date_ops)

            log(spark_session).info("Number of rows: "+str(df.count()))

            try:
                df \
                    .write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql:"+postgresql_access_dict["database"]) \
                    .option("dbtable", "public."+tbl_name) \
                    .option("user", postgresql_access_dict["username"])\
                    .option("password", postgresql_access_dict["password"]) \
                    .mode("append") \
                    .save()
            except Exception as e:
                log(spark_session).error("Error on writing database... ")
                log(spark_session).error(e)
        
        except Exception as e:
            log(spark_session).error(e)

    def read_file_and_store_on_datalake(spark_session, output_base_dir):
        """
            Read a traditional JSON ([{"a":1, "b":2}, {...}]) file 
            and store it into a postgresql database 

            Parameters
            ----------

            spark_session: SparkSession
                The SparkSession of the application

            file: str
                The file path of the JSON

            tbl_name: str
                The name of the table that is going to be
                created on postgresql

            schema: StructuredType(Array(StructuredField))
                The Spark schema ready to be used to con-
                vert JSON into a Spark Dataframe

            postgresql_access_dict: dict
                The dictionary with the data necessary to 
                access the postgresql. The json must contain
                {"database":..., "username":..., "password":...}

            date_ops: dict, optional
                The dictionary with the options necessary
                to parse string date into a timestamp

        """
        file = self._args[2]
        output_data_name = self._args[3]
        output_path = output_base_dir+"/"+output_data_name
        schema_json = self._args[4]
        date_ops = json.loads(self._args[5]) if len(self._args) == 6 else None

        ##################################
        #
        ## Parse schema JSON to a Spark schema
        #
        ##################################
        schema = get_schema(schema_json)
        log(spark).info("Schema: "+str(schema))

        try:
            df = spark_session.read.schema(schema).json(file)

            log(spark_session).info("Number of rows: "+str(df.count()))

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

    def generate_pivot_file_user_x_category_page(spark_session, file, output_path):
        return 1