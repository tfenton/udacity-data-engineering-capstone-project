# Introduction
In this capstone project I set out to derisk and sketch out a solution to a problem I face at work. There is a daily dump of metadata files which cover email usage. These need to be blended with employee data and made available in ElasticSearch to some analysts. Since ElasticSearch doesn't support joins (all data must be flattened and denormalized) blending must be done in some sort of ETL tool. Currently this is via a python script. I decided for my capstone project to make this process more robust, repeatable and verifiable by performing the ETL in airflow and using postgress to do the daily data join prior to pushing it to ElasticSearch.
# Data
Since the real data from work is sensitive, I used a python library named "faker" to create synthetic data for this project. I was able to recreate the syntax of the logs I want to work with but populating all fields with fake data. The data comes in CSV format and the volume is several million rows per day. I tested my work flow with multiple days of small synthetic data files as well as one large 1M record data file (generating the synthetic data was the bottle neck to creating even more volume so I capped out at what I knew I needed to prove the viablity of the pipeline.

I've uploaded a single day of data (for 2019-09-01) and the synthetic employees file. I didn't put the 1M record file or the full 4 months worth of smaller files I used for testing, into my git repo as that would put me well over my quota limit.

To generate synthetic data there's a Jupyter notebook called "Synthetic Proofpoint Data.ipynb" which has configurable parameters allowing you to change the size of the amount of data generated (DATA_SIZE) as well as the date range (START_DATE, END_DATE) over which the data is created.

All data was staged in Postgres. I installed Postgres 9.2 on a Centos machine I could access from Airflow.

There's also a notebook called "Test Postgress.ipynb" which I used to test the SQL I was placing into the various airflow operators. It provided only as a development artifact.
# Scaling
ElasticSearch is a tool used in our company for log file analysis. Since over time the size of the email log data has grown to on the order of a billion records, staging all the data in Postgres is not an option. But ElasticSearch's speed comes at the cost of doing joins, something a data store like postgres excels at. While the existing in memory Python solution of joining the employee data with the email log data is working, moving to an on disk solution (which is what I did here by doing the data blending in Postgres) is more sustainable and efficient as the data volume grows. And it also allows out of memory processing which means this pipeline can be deployed on a smaller virtual machine, another plus!

Spark would be an ideal replacement for ElasticSearch as it would allow the blending in place, without having to involve Postgres, even as the volume of data grows to over a billion records. But my work problem is currently deployed on ElasticSearch and changing that is not within the scope of my job currently.

# Airflow
I installed Airflow on a Centos machine and scheduled a pipleline to process the log files daily. Here's what the flow looks like:
1) Find the log for for the day of the run
2) Load employee records if needed (this is only done once or if the employees.csv file changes, there's a check to see if that files misses an expected size in which case the file is reloaded)
3) In parallel with 2 load the daily log data and then verify it loaded
4) Blend the data from steps 2 and 3
5) Upload the data to ElasticSearch