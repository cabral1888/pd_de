export JAVA_HOME=/home/igor/spark_env/java/jdk1.8.0_241
export DATA_DIR_1=/home/igor/codes/pd_de/data/BASEA/
export STREAMING_DATA_DIR_1=/home/igor/codes/pd_de/data/datalake
export STREAMING_OUTPUT_INTERVAL='1 minute'
export CHECKPOINT_DIR=/home/igor/codes/pd_de/checkpoint
export PYTHONPATH=jars/*

log4j_setting="-Dlog4j.configuration=file:log4j.properties"

rm -rf checkpoint/

zip -r dependencies.zip etl/ utils/ streaming/

#spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" --files log4j.properties --driver-class-path jars/* --py-files dependencies.zip run_etl.py universities.json universities '{"Id":"integer", "Name":"string"}' 

#spark-submit --driver-class-path jars/* --py-files dependencies.zip run_etl.py students.json students '{"Id":"string", "RegisteredDate":"timestamp", "State":"string", "City":"string", "UniversityId":"string", "CourseId":"string", "SignupSource":"string"}' '{"date_column": "RegisteredDate", "date_format":"yyyy-MM-dd HH:mm:ss"}'

#spark-submit --driver-class-path jars/* --py-files dependencies.zip run_etl.py subscriptions.json subscriptions '{"StudentId":"string", "PaymentDate":"timestamp", "PlanType":"string"}' '{"date_column": "PaymentDate", "date_format":"yyyy-MM-dd HH:mm:ss"}'

#spark-submit --driver-class-path jars/* --py-files dependencies.zip run_etl.py subjects.json subjects '{"Id":"integer", "Name":"string"}' 

#spark-submit --py-files dependencies.zip run_streaming.py "students" '{"Id":"string", "RegisteredDate":"timestamp", "State":"string", "City":"string", "UniversityId":"string", "CourseId":"string", "SignupSource":"string"}' '{"date_column": "RegisteredDate", "date_format":"yyyy-MM-dd HH:mm:ss"}'

#spark-submit --py-files dependencies.zip run_streaming.py "student_follow_subject" '{"StudentId":"string", "SubjectId":"integer", "FollowDate":"timestamp"}' '{"date_column": "FollowDate", "date_format":"yyyy-MM-dd HH:mm:ss"}'
