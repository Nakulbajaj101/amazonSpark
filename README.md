# Sparkify Music Analytical Engine

The purpose of the project is to build a database and design an
ETL pipeline that would power the analytical needs for Sparkify.

## Motivation

A startup called Sparkify wants to analyze the data they've been
collecting on songs and user activity on their new music streaming app.
The analytics team is particularly interested in understanding what
songs users are listening to. Currently, they don't have an easy way
to query their data, which resides in a directory of JSON logs on user
activity on the app, as well as a directory with JSON metadata on the
songs in their app. In order to provide better service to their users,
Sparkify would like to bring their data to a data store/database so
analysts can perform queries faster, and analyse user patterns in
an optimized manner.


## Built With

The section covers tools used to enable the project.

1. Amazon EMR process data and output to s3
2. PySpark to process and carry out ETL
3. Bash to create the infrastructure and delete the infrastructure

## Tables

### Fact Table

1. songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

2. users - users in the app
- user_id, first_name, last_name, gender, level

3. songs - songs in music database
- song_id, title, artist_id, year, duration

4. artists - artists in music database
-  artist_id, name, location, latitude, longitude

5. time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday


## Project Files and Folders

1. etl.py - Contains all the logic to extract data from S3 and process data on spark and then load data as parquet files into the s3 folder and region specified by user.
2. sparkSubmit.sh - Shell file that will get executed on the emr cluster and will call the spark submit on the cluster and will run the etl.py
3. createCluster.sh - Contains the pipeline to automate infrastructure requirements which will create the emr cluster, load the etl.py file on the cluster and load data from udacity s3 into user specific s3 bucket
4. terminateCluster.sh - Contains the pipeline to destroy the infratsructure associated with the project.

## Extra files

The cities.csv and sparkTest.py are used to do some testing, and can be used to do some practicing.
The examples folder contains ipython and .py file on NLP example on spark

## Running the ETL pipeline

1. Create the editor role in aws iam
2. Configure the aws cli with the editor role access key and secret.
3. Create the ssh key pair for ec2 instance using aws cli, give it a name such as my-key-pair. Make sure key is stored in root directory and is in the same region in which emr cluster/ec2 instances will be created.
   `aws ec2 create-key-pair --key-name my-key-pair --query "KeyMaterial" --output text > my-key-pair.pem`

4. If you're connecting to your instance from a Linux computer, it is recommended that you use the following command to set the permissions of your private key file so that only you can read it.
   `chmod 400 MyKeyPair.pem`
   
5. Open terminal
6. Run createCluster.sh script to create the emr cluster and execute the spark job on the cluster
   .Pass the cluster name as first choice of argument and name of the key associated with ec2 instance

    `bash createCluster.sh <cluster_name> <keyName>`


## Destroying the infrastructure to avoid costs

1. Run the terminateCluster.sh that will terminate the cluster
   
    `bash terminateCluster.sh`
 
# Contact for questions or support

Nakul Bajaj
