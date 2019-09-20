# Introduction
Music streaming startup Sparkify, want to move their processes and data onto the cloud. Currently their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project builds an ETL pipeline using Apache Airflow that extracts the data from S3, stages it in a Redshift cluster on AWS, and then creates fact and dimension tables from this staged data. This allows the analytics team to continue finding insights in what songs their users are listening to.
# Setup and Usage
1. Spin up a redshift cluster in AWS
2. Create a connection to AWS which will be used by Airflow to retrieve the JSON data from S3. In the Airflow UI do the following:
  * Click the "Admin" menu
  * Click the "Connections" menu item
  * Click "Create"
  * Enter "aws_credentials" for the Conn ID
  * Enter "Amazon Web Services" for the Conn Type
  * Enter the IAM key for the Login
  * Enter the IAM secret for the Password
  * This IAM user must have S3 access.
3. Create a connection to the Redshift cluster you spun up in step 1 which will be used by Airflow to stage the JSON data from S3. In the Airflow UI do the following:
  * Click the "Admin" menu
  * Click the "Connections" menu item
  * Click "Create"
  * Enter "redshift" for the Conn ID
  * Enter "Postgres" for the Conn Type
  * Enter the username you supplied when you created the cluster for the Login
  * Enter the password you supplied when you created the cluster for the Password
  * Enter 5439 for the Port
  * Enter the cluster FQDN for the Host
4. Execute the ```create_tables.sql``` queries, found in the root of this project, to setup the schema for the ETL.
5. Turn on the DAG ```udac_example_dag``` in Airflow.

At this point the data is populated and ready for analysis.
