import configparser
from datetime import datetime
import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

    
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function initiates a spark session on AWS hadoop
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function loads song_data from S3, extracts songs and artist tables
    and then loads it back to S3
        
    Parameters:
        spark       : spark Session
        input_data  : song_data file location to extract songs and artist tables
        output_data : the location to save the output data
            
    """
    print("Start processing song_data JSON files...")
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = input_data + 'song_data/A/B/C/TRABCEI128F424C983.json'
    
    print("Reading song_data files from {}...".format(song_data))
    # read song data file
    df = spark.read.json(song_data).drop_duplicates()
    
    print("Song_data schema:")
    df.printSchema()
    
    # created song view to write SQL Queries
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, 
                            title,
                            artist_id,
                            year,
                            duration
                            FROM song_data_table
                            WHERE song_id IS NOT NULL
                            """)
    
    print("Songs_table schema:")
    songs_table.printSchema()
    
    print("Writing songs_table parquet files to {}..."\
        .format(output_data+'songs_table'))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')
    
    print("finished writing songs_table")

    # extract columns to create artists table
    artists_table = spark.sql("""
                              SELECT DISTINCT artist_id, 
                              artist_name,
                              artist_location,
                              artist_latitude,
                              artist_longitude
                              FROM song_data_table 
                              WHERE artist_id IS NOT NULL
                              """)
          
    artists_table.printSchema()
    artists_table.show(5, truncate=False)

    print("Writing artists_table parquet files to {}..."\
        .format(output_data+'artists_table'))
              
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    Description: This function loads logs data, processes user and time data and then loads back
    data to  S3
        
    Parameters:
        spark       : spark Session
        input_data  : log_data file location to load the source data
        output_data : the location to save the output 
            
    """
    print("Start processing log_data JSON files...")
          
    # get filepath to log data file
    log_data =input_data + 'log_data/*/*/*events.json'
    #log_data =input_data + 'log_data/*/*/2018-11-01-events.json'
    
    print("Reading log_data files from {}...".format(log_data))
          
    # read log data file
    df = spark.read.json(log_data).drop_duplicates()
          
    print("...finished reading log_data")
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT userId as user_id, 
                            firstName as first_name,
                            lastName as last_name,
                            gender as gender,
                            level as level
                            FROM log_data_table 
                            WHERE userId IS NOT NULL
                            """)
          
    print("Users_table schema:")
    users_table.printSchema()
    print("Users_table examples:")
    users_table.show(5)
    
    print("Writing users_table parquet files to {}..."\
            .format(output_data+'users_table/'))
          
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')
          
    print("...finished writing users_table")
          
    print("Creating timestamp column...")
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    
    print("Creating datetime column...")
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: F.to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    df.createOrReplaceTempView("log_data_table_time")
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                            start_time,
                            hour(start_time) as hour,
                            dayofmonth(start_time) as day,
                            weekofyear(start_time) as week,
                            month(start_time) as month,
                            year(start_time) as year,
                            dayofweek(start_time) as weekday
                            FROM
                            log_data_table_time
                            WHERE ts IS NOT NULL
                            """)
    print("Time_table schema:")
    time_table.printSchema()
    print("Time_table examples:")
    time_table.show(5)
          
    print("Writing time_table parquet files to {}..."\
            .format(output_data+'time_table/'))
          
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    print("Reading song_data files from {}...".format(output_data+'songs_table/'))
          
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')

    print("Joining log_data and song_data DFs...")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT 
                                monotonically_increasing_id() as songplay_id,
                                logTb.start_time,
                                month(start_time) as month,
                                year(start_time) as year,
                                logTb.userId as user_id,
                                logTb.level as level,
                                songTb.song_id as song_id,
                                songTb.artist_id as artist_id,
                                logTb.sessionId as session_id,
                                logTb.location as location,
                                logTb.userAgent as user_agent
                                FROM log_data_table_time logTb
                                JOIN song_data_table songTb on logTb.artist = songTb.artist_name and logTb.song = songTb.title
                                """).drop_duplicates()
    print("Songplays_table schema:")
    songplays_table.printSchema()
    print("Songplays_table examples:")
    songplays_table.show(5, truncate=False)
          
    print("Writing songplays_table parquet files to {}..."\
            .format(output_data+'songplays_table/'))
          
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')
    print("...finished writing songplays_table")

def main():
    """
    Main program
    Will call the process_song_data and process_log_data functions
    to create all the OLAP oriented dimensions and fact tables and
    store them as parquet files for later use.
    Stops the spark session when done.
    """
    print("\nSTARTED ETL pipeline (to process song_data and log_data) \
            at {}\n".format(datetime.now()))
          
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://s3-buck-demo1/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    print("Finished the ETL pipeline processing.")
    print("ALL DONE.")
    print("FINISHED ETL pipeline (to process song_data and log_data) at {}"\
            .format(datetime.now()))         

if __name__ == "__main__":
    main()
