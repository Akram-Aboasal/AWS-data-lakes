# AWS-data-lakes-with-spark

# Project: Spark and Data Lake

This project builds an ETL pipeline for a data lake. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app. We loaded data from S3, process the data into analytics tables using Spark, and load them back into S3.

## Project Structure

Spark and Data Lake

sparkify.ipynb              # ETL builder

dl.cfg                      # AWS configuration file


# How to Run

    Add AWS credentials in dl.cfg

    Please do not put your access/secret key in public
    If you don't have an IAM user, you must create one then create your own s3 bucket on IAM user to upload data on it and refer the url 
    into input_data and output_data
    [AWS]
    AWS_ACCESS_KEY_ID = [your access key]
    AWS_SECRET_ACCESS_KEY = [your secret key]
    
    first, you run EMR cluster (using 1 Master node and 2 Core nodes), then run Pyspark notebook via JupyterLab and the following notebook


# ELT Pipeline
sparkify.ipynb

ELT pipeline builder

    process_song_data
        Load raw data from S3 buckets to Spark stonealone server and process song dataset to insert record into songs and artists dimension table

    process_log_data
        Load raw data from S3 buckets to Spark stonealone server and Process event(log) dataset to insert record into time and users dimensio table and songplays fact table

# Database Schema

### songplays table

|songplays| 	type|
|--------|----------|
|songplay_id |	INT|
|start_time |	TIMESTAMP|
|user_id 	|INT|
|level 	|VARCHAR|
|song_id 	|VARCHAR|
|artist_id |	VARCHAR|
|session_id 	|INT|
|location |	TEXT|
|user_agent |	TEXT|

### user table

|users |	type|
|---------|--------|
|user_id |	INT|
|first_name |	VARCHAR|
|last_name |	VARCHAR|
|gender |	CHAR(1)|
|level |	VARCHAR|

### songs table

|songs |	type|
|---------|--------|
|song_id |	VARCHAR|
|title |	VARCHAR|
|artist_id |	VARCHAR|
|year |	INT|
|duration |	FLOAT|

### artists table

|artists |	type|
|---------|--------|
|artist_id| 	VARCHAR|
|name |	VARCHAR|
|location |	TEXT|
|latitude |	FLOAT|
|logitude |	FLOAT|

### time table

|time |	type|
|---------|--------|
|start_time |	TIMESTAMP|
|hour 	|INT|
|day 	|INT|
|week 	|INT|
|month 	|INT|
|year 	|INT|
|weekday |	VARCHAR|
