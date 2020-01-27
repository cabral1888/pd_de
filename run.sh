export JAVA_HOME=/home/igor/spark_env/java/jdk1.8.0_241
export DATA_DIR_1=/home/igor/codes/pd_de/data/BASEA/
export PYTHONPATH=jars/*

zip -r dependencies.zip etl/ utils/

spark-submit --driver-class-path jars/* --py-files dependencies.zip run_etl.py
