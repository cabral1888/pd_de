from pyspark.sql import SparkSession

import logging
import os
import json

from etl.execute_etl import read_file_and_store_on_postgresql
from utils.schema_utils import get_schema

data_dir = os.environ.get("DATA_DIR_1", "/home/igor/codes/pd_de/data/BASEA/")
filename = data_dir+"courses.json"
table_name = "courses"

spark = SparkSession.builder \
     .master("local") \
     .appName(filename) \
     .getOrCreate()

schema_courses = get_schema('{"Id":"integer", "Name":"string"}')
logging.info("Schema course: "+str(schema_courses))
#read_file_and_store_on_postgresql(spark, filename, table_name, schema_courses)

schema_student = get_schema('{"Id":"string", "RegisteredDate":"timestamp", "State": "string", "City": "string", "UniversityId": "integer", "CourseId": "integer", "SignupSource": "string"}')
logging.info("Schema student: "+str(schema_student))
filename = data_dir+"students.json"
table_name = "students"
#read_file_and_store_on_postgresql(spark, filename, table_name,  schema_student, {"date_column": "RegisteredDate", "date_format":"yyyy-MM-dd HH:mm:ss.S"})

schema_session = get_schema('{"StudentId":"string", "SessionStartTime":"timestamp", "StudentClient": "string"}')
logging.info("Schema session: "+str(schema_session))
filename = data_dir+"sessions.json"
table_name = "sessions"
read_file_and_store_on_postgresql(spark, filename, table_name,  schema_session, {"date_column": "SessionStartTime", "date_format":"yyyy-MM-dd HH:mm:ss"})
