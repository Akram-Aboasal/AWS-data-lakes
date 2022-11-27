import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]=config['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Reading song data and create songs and artists table
    
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/"
    
    # read song data file
    df = spark.read.json(song_data)
    df.count()

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).distinct()
    print(songs_table.show(5, False))
    
    # write songs table to parquet files partitioned by year and artist
 
    songs_table.write.mode("overwrite").parquet(output_data+'songs/'+'songs.parquet', partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).distinct() 
    print(artists_table.show(5, truncate = False))
    # write artists table to parquet files
 
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/' + 'artists.parquet', partitionBy=['artist_id'] )


def process_log_data(spark, input_data, output_data):
    """ Reading log data and create songs and artists table
    
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """
    # get filepath to log data file
    log_data =input_data + "log-data/"

    # read log data file
    log_df = spark.read.json(log_data)
    print(log_df.show(2))
    
    # filter by actions for song plays
    log_df = log_df.where(log_df['page'] == 'NextSong')
    print(log_df.show(2))
    
    # extract columns for users table    
    users_table = log_df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    print(users_table.show(5, truncate = False))
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users/' + 'users.parquet', partitionBy = ['userId'])
    
    # create timestamp column from original timestamp column
    log_df = log_df.withColumn('timestamp',( (log_df.ts.cast('float')/1000).cast("timestamp")))
    
    # extract columns to create time table
    time_table = log_df.select(
                    F.col("timestamp").alias("start_time"),
                    F.hour("timestamp").alias('hour'),
                    F.dayofmonth("timestamp").alias('day'),
                    F.weekofyear("timestamp").alias('week'),
                    F.month("timestamp").alias('month'), 
                    F.year("timestamp").alias('year'), 
                    F.date_format(F.col("timestamp"), "E").alias("weekday")
                )


    time_table.show(5, False)

    # write time table to parquet files partitioned by year and month
   
    time_table.write.mode("overwrite").parquet(output_data + 'time/' + 'time.parquet', partitionBy=['year','month'])


    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df, (log_df.song == song_df.title) & (log_df.artist == song_df.artist_name) & (log_df.length == song_df.duration), how='inner')
    songplays_table = songplays_table.distinct() \
                        .select("userId", "timestamp", "song_id", "artist_id", "level", "sessionId", "location", "userAgent" ) \
                        .withColumn("songplay_id", F.row_number().over( Window.partitionBy('timestamp').orderBy("timestamp"))) \
                        .withColumnRenamed("userId","user_id")        \
                        .withColumnRenamed("timestamp","start_time")  \
                        .withColumnRenamed("sessionId","session_id")  \
                        .withColumnRenamed("userAgent", "user_agent") \
    # write songplays table to parquet files partitioned by user_id
    print(songplays_table.show(5))
    songplays_table.write.mode("overwrite").parquet(output_data + 'songplays/' + 'songplays.parquet',partitionBy=['user_id'])


def main():
    spark = create_spark_session()
    input_data = "s3a://sparkifybucket1/"
    output_data = "s3a://sparkifybucket1/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
