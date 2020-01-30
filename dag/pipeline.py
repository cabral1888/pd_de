from airflow import DAG
from airflow.operators import BashOperator

from datetime import datetime
from dateutil.relativedelta import relativedelta

######################################
#
## Airflow DAG
#
######################################

default_args = {
    "depends_on_past": False,
    "start_date": datetime.now() - relativedelta(days=1),
    "email_on_failure": False
}

######################################
#
## DAG will runs on every day at midnight
#
######################################
dag = DAG("pd_pipeline", default_args=default_args, schedule_interval="0 0 * * *")

######################################
#
## Spark submit command definition
#
######################################
proj_home = "/home/igor/codes/pd_de"
virtualenv_path = "/home/igor/spark_env"

virtualenv_activate = "source " + virtualenv_path + "/bin/activate; "
shell_command = "spark-submit"
confs = '--conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}"'
files = '--files ' + proj_home + '/log4j.properties'
driver_class_path = '--driver-class-path ' + proj_home + '/jars/*'
py_files = '--py-files ' + proj_home + '/dependencies.zip ' + proj_home + '/run_etl.py'

command_base = "{} {} {} {} {} {}".format(virtualenv_activate, shell_command, confs, files, driver_class_path, py_files)

######################################
#
## Environments variables definition
#
######################################
env = {
    "JAVA_HOME": virtualenv_path + "/java/jdk1.8.0_241",
    "DATA_DIR_1": proj_home + "/data/",
    "DATALAKE_DATA_DIR_1": proj_home + "/data/datalake",
    "POSTGRESQL_DATABASE": "igoruchoa",
    "POSTGRESQL_USERNAME": "igoruchoa",
    "POSTGRESQL_PASSWORD": "igoruchoa",
    "log4j_setting": "-Dlog4j.configuration=file://" + proj_home + "/log4j.properties"
}

######################################
#
## Operators definition
#
######################################
copy_log4j = BashOperator(
    task_id="copy_log4j",
    bash_command="cp $PROJ_HOME/log4j.properties .",
    dag=dag,
    env={"PROJ_HOME": proj_home}
)

zip_dependencies = BashOperator(
    task_id="zip_dependencies",
    bash_command="cd $PROJ_HOME; zip -r dependencies.zip etl/ utils/ streaming/",
    dag=dag,
    env={"PROJ_HOME": proj_home}
)

param = 'etl /BASEA/universities.json universities \'{"Id":"integer", "Name":"string"}\''
universities = BashOperator(
    task_id="universities.json",
    bash_command=command_base + " " + param,
    dag=dag,
    env=env
)

param = 'etl /BASEA/subscriptions.json subscriptions \'{"StudentId":"string", "PaymentDate":"timestamp", "PlanType":"string"}\' \'{"date_column": "PaymentDate", "date_format":"yyyy-MM-dd HH:mm:ss"}\''
subscriptions = BashOperator(
    task_id="subscriptions.json",
    bash_command=command_base + " " + param,
    dag=dag,
    env=env
)

param = 'etl /BASEA/subjects.json subjects \'{"Id":"integer", "Name":"string"}\''
subjects = BashOperator(
    task_id="subjects.json",
    bash_command=command_base + " " + param,
    dag=dag,
    env=env
)

param = 'etl /BASEA/courses.json courses \'{"Id":"integer", "Name":"string"}\''
courses = BashOperator(
    task_id="courses.json",
    bash_command=command_base + " " + param,
    dag=dag,
    env=env
)

param = 'etl /BASEA/students.json students \'{"Id":"string", "RegisteredDate":"timestamp", "State":"string", "City":"string", "UniversityId":"string", "CourseId":"string", "SignupSource":"string"}\' \'{"date_column": "RegisteredDate", "date_format":"yyyy-MM-dd HH:mm:ss"}\''
students = BashOperator(
    task_id="students.json",
    bash_command=command_base + " " + param,
    dag=dag,
    env=env
)

param = 'datalake /BASEB events'
json_schema = '\'{\
  "Last Accessed Url": "string",\
  "Page Category": "string",\
  "Page Category 1": "string",\
  "Page Category 2": "string",\
  "Page Category 3": "string",\
  "Page Name": "string",\
  "at": "timestamp",\
  "browser": "string",\
  "carrier": "string",\
  "city_name": "string",\
  "clv_total": "integer",\
  "country": "string",\
  "custom_1": "string",\
  "custom_2": "string",\
  "custom_3": "string",\
  "custom_4": "string",\
  "device_new": "boolean",\
  "first-accessed-page": "string",\
  "install_uuid": "string",\
  "language": "string",\
  "library_ver": "string",\
  "marketing_campaign": "string",\
  "marketing_medium": "string",\
  "marketing_source": "string",\
  "model": "string",\
  "name": "string",\
  "nth": "integer",\
  "os_ver": "string",\
  "platform": "string",\
  "region": "string",\
  "session_uuid": "string",\
  "studentId_clientType": "string",\
  "type": "string",\
  "user_type": "string",\
  "uuid": "string"\
}\''
json_data_ops = '\'{"date_column": "at", "date_format":"yyyy-MM-dd HH:mm:ss"}\''

events = BashOperator(
    task_id="events",
    bash_command=command_base + " " + param + " " + json_schema + " " + json_data_ops,
    dag=dag,
    env=env
)

param = 'pivot datalake/events user_categories'
pivot = BashOperator(
    task_id="pivot",
    bash_command=command_base + " " + param,
    dag=dag,
    env=env
)

######################################
#
## Operators execution order
#
######################################
copy_log4j >> zip_dependencies >> [students, courses, universities, subjects] >> subscriptions >> events >> pivot
