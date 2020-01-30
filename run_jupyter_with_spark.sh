export JAVA_HOME=/home/igor/spark_env/java/jdk1.8.0_241
export SPARK_HOME=/home/igor/spark_env/lib/python3.6/site-packages/pyspark/
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PYSPARK_SUBMIT_ARGS="--jars ../jars/* --driver-class-path ../jars/*"

pyspark $PYSPARK_SUBMIT_ARGS