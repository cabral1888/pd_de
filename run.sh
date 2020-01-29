# Creating environment variables
export JAVA_HOME=/home/igor/spark_env/java/jdk1.8.0_241
export DATA_DIR_1=/home/igor/codes/pd_de/data
export STREAMING_DATA_DIR_1=/home/igor/codes/pd_de/data/datalake
export STREAMING_OUTPUT_INTERVAL='1 minute'
export CHECKPOINT_DIR=/home/igor/codes/pd_de/checkpoint
export PYTHONPATH=jars/*

# Poiting to the project log4j.properties file
log4j_setting="-Dlog4j.configuration=file:log4j.properties"

# Remove Structured Streaming checkpoint dir (in the case of code was modified)
rm -rf checkpoint/

# Generate the zip file with all the spark code dependencies
zip -r dependencies.zip etl/ utils/ streaming/

#EX1
# spark-submit commands in the case you need to run manually
# spark-submit commands to streaming part of the project

#spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" --files log4j.properties --driver-class-path jars/* --py-files dependencies.zip run_etl.py "posgresql" "/BASEA/universities.json" universities '{"Id":"integer", "Name":"string"}' 

#spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" --files log4j.properties --driver-class-path jars/* --py-files dependencies.zip run_etl.py "posgresql" "/BASEA/students.json" students '{"Id":"string", "RegisteredDate":"timestamp", "State":"string", "City":"string", "UniversityId":"string", "CourseId":"string", "SignupSource":"string"}' '{"date_column": "RegisteredDate", "date_format":"yyyy-MM-dd HH:mm:ss"}'

#spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" --files log4j.properties --driver-class-path jars/* --py-files dependencies.zip run_etl.py "posgresql" "/BASEA/subscriptions".json subscriptions '{"StudentId":"string", "PaymentDate":"timestamp", "PlanType":"string"}' '{"date_column": "PaymentDate", "date_format":"yyyy-MM-dd HH:mm:ss"}'

#spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" --files log4j.properties --driver-class-path jars/* --py-files dependencies.zip run_etl.py "posgresql" "/BASEA/subjects.json" subjects '{"Id":"integer", "Name":"string"}' 

#spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" --files log4j.properties --py-files dependencies.zip run_streaming.py "sessions" '{"StudentId":"string", "SessionStartTime":"timestamp", "StudentClient":"string"}' '{"date_column": "SessionStartTime", "date_format":"yyyy-MM-dd HH:mm:ss"}' &

#spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" --files log4j.properties --py-files dependencies.zip run_streaming.py "student_follow_subject" '{"StudentId":"string", "SubjectId":"integer", "FollowDate":"timestamp"}' '{"date_column": "FollowDate", "date_format":"yyyy-MM-dd HH:mm:ss"}' &

#EX2
spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" --files log4j.properties --py-files dependencies.zip run_etl.py "datalake" "/BASEB" "events" '{"Last Accessed Url": "string","Page Category": "string","Page Category 1": "string","Page Category 2": "string","Page Category 3": "string","Page Name": "string","at": "timestamp","browser": "string","carrier": "string","city_name": "string","clv_total": "integer","country": "string","custom_1": "string","custom_2": "string","custom_3": "string","custom_4": "string","device_new": "boolean","first-accessed-page": "string","install_uuid": "string","language": "string","library_ver": "string","marketing_campaign": "string","marketing_medium": "string","marketing_source": "string","model": "string","name": "string","nth": "integer","os_ver": "string","platform": "string","region": "string","session_uuid": "string","studentId_clientType": "string","type": "string","user_type": "string","uuid": "string"}' '{"date_column": "at", "date_format":"yyyy-MM-dd HH:mm:ss"}' &
