# Overview
The application consists in a Data pipeline created to allow Business Inteligence Analysis and statistical/machine learning in scale. What have been proposed was to estabilish a entire archutecture that take advantage of scalability and deliveries the best performance om the job execution.

Essentially, the application can be divided in two parts: (1) batch processing and (2) streaming processing. 

## Batch processing
Some of the ETLs necessary to perform analysis do not have real-time requirements, so they can live in a relational database and when necesary, it can be updated or deleted. The data files that fits here is: students.json, courses.json, universities.json, etc. The choice for the project here was **PostgreSQL**.

## Streaming processing
Here, the data have a major requirement: be processed as fast as possible, in real-time (or near real-time). In order ro accomplish this requirement, we need to dive in streaming solutions, and in the case of this project, it was choosen **Spark Structured Streaming** constantly outputting data into a data lake structure. Due to time constraint, it was only possible to read streaming data from a socket, but the source can be changed to a broker (like Kafka) without any problem.

## Architecture
The project was based on lambda architecture pattern as follows:

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
## Running through shell
You can run either batch analysis or streaming by command line. Just look at the file `run.sh` and see your content. There is a lot of `spark-submit` commands, one per json file or streaming ingestion. You just need to choose some of them, uncoment it, and type:
```
./run.sh
```

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
4. On the UI, click on `DAGS` tab, look for `pd_pipeline` and enable it.
5. The same spaek-submit commands you have just saw in the previous session afe going to run every day at mid-night. You can hurry up by pressing `schedule now`

PS.: Apache airflow will not start streaming jobs on thiz project. In this case, it is only possible through shell command line.
