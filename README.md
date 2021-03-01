# Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

project requires building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## Project Description

Apply learnings of spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, load data from S3, process the data into analytics tables using Spark, and load them back into S3. Deploy this Spark process on a cluster using AWS.

## Project Datasets

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data
* output json path: s3://udacity-demo/


## Schema for Song Play Analysis


### Fact Table

* songplays - records in log data associated with song plays i.e. records with page NextSong
      songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

* users - users in the app
      user_id, first_name, last_name, gender, level
* songs - songs in music database
      song_id, title, artist_id, year, duration
* artists - artists in music database
      artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units
      start_time, hour, day, week, month, year, weekday

## Document Process
Created star schema model with fact table "songplays" along with four dimension tables named "users_table" , "artists_table", "songs_table" and "time_table". Developed an ETL pipeline to read all information from JSON file to the spark cluster, processed the data and saved the results back to S3 (for analytics and reports).


### Scripts

There are 3 python scripts with desicription as below :

* "dl.cfg" - Configuration file for AWS IAM credentials
* "Notebook.ipynb" - This notebook is used to test the dataset and confirm the python script statements used in etl.py.
* "Etl.py" - This script is used to extract the data from S3 , process it in spark (without the need of staging tables) and write back to S3
* "Notebook_ETL.pynb" - This notebook is used to test etl.py script with smaller dataset.



## Instructions

1. Update  dl.cfg for AWS IAM Credentials
2. Run the etl.py :
    1. In local mode by running  %run etl.py in the local notebook
    2. Run the Python script from AWS CLI(SSH Connection) to master node of EMR 
    3. Create notebook in AWS EMR and run the python script

