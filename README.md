# Overview
The application consists in a Data pipeline created to allow Business Inteligence Analysis and statistical/machine learning in scale. What have been proposed was to estabilish a entire archutecture that take advantage of scalability and deliveries the best performance om the job execution.

Essentially, the application can be divided in two parts: (1) batch processing and (2) streaming processing. 

## Batch processing
Some of the ETLs necessary to perform analysis do not have real-time requirements, so they can live in a relational database and when necesary, it can be updated or deleted. The data files that fits here is: students.json, courses.json, universities.json, etc. The choice for the project here was **PostgreSQL**.

## Streaming processing


