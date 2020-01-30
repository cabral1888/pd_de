[[_TOC_]]
# Overview
The application consists of a Data pipeline created to allow Business Intelligence Analysis and statistical/machine learning in scale. What has been proposed was to establish an entire architecture that takes advantage of scalability and deliveries the best performance om the job execution.

Essentially, the application can be divided in two parts: (1) batch processing and (2) streaming processing. 

## Batch processing
Some of the ETLs necessary to perform analysis do not have real-time requirements, so they can live in a relational database and when necessary, it can be updated or deleted. The data files that fits here is: students.json, courses.json, universities.json, etc. The choice for the project here was **PostgreSQL**.

## Streaming processing
Here, the data have a major requirement: be processed as fast as possible, in real-time (or near real-time). In order ro accomplish this requirement, we need to dive in streaming solutions, and in the case of this project, it was chosen **Spark Structured Streaming** constantly outputting data into a data lake structure. Due to time constraints, it was only possible to read streaming data from a socket, but the source can be changed to a broker (like Kafka) without any problem.

## Architecture
The project was based on the lambda architecture pattern as follows:

```
json_files                 Socket
    |                      Broker, etc
    |                        |
    V                        V
 SparkSQL             Structured Streaming
    |                        |
    |                        |
    V                        V
PostgreSQL                Data lake
     \                       /
      \                     /
       \                   /
        \                 /
          BI+Statistic/ML
              Analysis
```            
# Running the code
## Prerequisites
### Python virtual environment
To run the application properly, you must define a minimum environment able to run the entire pipeline
of the application. In the project root folder, you will find the requirements.txt containing all the
necessary Python dependencies to run the application. In our case, we are going to use Python Virtual
Environment to keep project dependencies isolated from the host OS. If you do not have Virtual Environment 
installed, you can install it by  typing the following command on terminal:
```shell
# Install Python if you do not have already installed it 
$ sudo apt-get install python3.6
$ sudo apt install virtualenv
```

Inside project home, create a virtual environment:
```shell script
$ virtualenv -p /usr/bin/python3.6 venv
```

Initialize virtual environment:
```shell script
$ source venv/bin/activate
```

After typing this command, you must be able to see the prompt `(venv)`. Here, you can install the python
dependencies of `requirements.txt`:
```shell script
$ pip install requirements.txt
```
### Postgresql
The application uses PostgreSQL in order to store data. You will need a running instance of PostgreSQL; in
the case you do not have it, you can install an instance of PostgreSQL in your host machine. Here are the
necessary steps to perform this action:

Install PostgreSQL:
```shell script
$ sudo apt install postgresql postgresql-contrib
```

Create a role:
```shell script
# Log with postgres user
$ sudo -i -u postgres

# Create a user for you 
$ createuser --interactive (choose USERNAME and y)

# Create a database for your user
$ createdb USERNAME

# Comeback to a sudo user (Ctrl + D or exit)

# Add the username created before to the OS system (fill the form)
$ sudo adduser USERNAME

# Login to the system with this user
$ sudo -i -u USERNAME

# Log into the postgres console
$ psql
``` 

### Java
You must install the Java 8 in your host machine. The command:
```shell script
sudo apt install openjdk-8-jdk
``` 
Optionally, you can use the tar.gz file from oracle (you must sign up on the website before):
```shell script
https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
```
You must change the variable JAVA_HOME (either in .sh scripts and in the Airflow DAG) according to Java location in your machine

## Environment variables
When you run this code, you must fill some environment variables according to your scenario (in shell_script and airflow dag). These variables are:
* JAVA_HOME: java root path location
* DATA_DIR_1: input directory where live the data provided by PD (root path which contains BASEA and BASEB, whitespaces have been removed)
* STREAMING_DATA_DIR_1: output data of the streaming application
* STREAMING_OUTPUT_INTERVAL: interval of streaming output data
* CHECKPOINT_DIR: checkpoint necessary to the streaming application
* POSTGRESQL_DATABASE: postgresql database name
* POSTGRESQL_USERNAME: postgresql username
* POSTGRESQL_PASSWORD: postgresql password

On Airflow dag file (`pipeline.py`) you must change the value of the following variables:
* proj_home: project (`pd_de`) root folder
* virtualenv_path = virtual environment root folder (`venv/`)
## Running through a shell
You can run either batch analysis or streaming by command line. Just look at the file `run.sh` and see your content. There is a lot of `spark-submit` commands, one per JSON file or streaming ingestion. You just need to choose some of them, uncomment it, and type:
```
# (venv) means inside your virtual environment
(venv)$ ./run.sh
```
PS.: Remember to edit the script setting the environment variables according to your scenario.

### Running streaming code
In order to run the streaming code, you must create a Linux socket:
```shell script
# The port used in the project was 9999
$ nc -lk 9999
```

After created, you can send data through it (each JSON unit by line) and 
this data will be stored on datalake structure

## Running through Apache Airflow
Considering you already have installed Apache Airflow in the host computer, all you need to do is:
1. Copy the file inside `dags/` dir and put it on the following directory:
```
~/airflow/dags
```
2. Start airflow scheduler by typing:
```
airflow scheduler
```
3. Finally, start airflow web UI:
```
airflow webserver
```
4. On the UI, click on `DAGS` tab, look for `pd_pipeline` and enable it. You should see a screen like this:
![img](img/Screenshot%20from%202020-01-30%2015-24-23.png)


5. The same spark-submit commands you have just saw in the previous session are going to run every day at midnight. You can hurry up by pressing `Trigger now`

PS.: Apache airflow will not start streaming jobs on this project. In this case, it is only possible through the shell command line.
PS2.: Remember to edit the script setting the environment variables according to your scenario.

# Analysis
Some analysis was performed using data provided by PD. It was executed using the Jupyter notebook tool and SparkML. You can find
the jupyter files in the `analysis_notebooks/` folder. Additionally, to improve the usability of the system, it was developed a 
script to allow users to start a Jupyter notebook server using a predefined SparkSession. You just need to execute the `run_jupyter_with_spark.sh`
script and it will launch a Jupyter integrated with Spark.

PS.: Remember to edit the script setting the environment variables according to your scenario. 
