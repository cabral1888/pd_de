import json
from utils.logging_utils import log

from utils.schema_utils import convert_date_using_data_ops_schema

def read_file_and_store_on_postgresql(spark_session, file, tbl_name, schema, postgresql_access_dict, date_ops={}):
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