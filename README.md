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
