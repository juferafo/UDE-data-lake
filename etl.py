# TODO LIST
# Create an S3 bucket -> include this in the README.md
# Write data in different folders in S3
# Test this code with EMR
# Include a flag/s to allow local and cloud functionality for input and output data
# drop NaN

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, from_unixtime, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from lib import create_spark_session
from lib import sch_song_data, sch_log_data
from lib import write_to_parquet


def process_song_data(spark, input_data, output_data):
    """
    This method process the song dataset and extracts the fields corresponding to the 
    songs and artists tables. It writes these tables in parquet format in 
    the output path provided in the argument "output_data"
    
    Args:
        spark (pyspark.sql.session.SparkSession): SparkSession object
        input_data (str): Path (local or cloud) where the input data is located.
        output_data (str): Path (local or cloud) where the output data will be located.
        
    Returns:
        df_song (): song dataset DataFrame
    """
    
    global df_song, song_data
    
    # Full set of song data
    #song_data = os.path.join(input_data, "song-data/*/*/*.json")
    
    # Subset A/A/A/*.json of song data for testing purposes
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")

    df_song = spark.read.json(song_data, schema = sch_song_data)

    # songs table
    songs_table = df_song.select("song_id", "title", "artist_id", "year", "duration")
    
    # songs table written to parquet file partitioned by year and artist
    songs_out = os.path.join(output_data, "songs.parquet")
    write_to_parquet(dataframe = songs_table,\
                     path = songs_out,\
                     mode = "overwrite",\
                     partition_cols = ["year", "artist_id"])
    
    # artists table
    artists_table = df_song.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    artists_out  = os.path.join(output_data, "artists.parquet")
    
    # artists table written to parquet files
    write_to_parquet(dataframe = artists_table,\
                     path = artists_out,\
                     mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This method process the log dataset and extracts the fields to build the songplays, 
    users and time data. Afterwards, it writes the tables in parquet format in the 
    indicated path "output_data"
    
    Args:
        spark (pyspark.sql.session.SparkSession): SparkSession object
        input_data (str): Path (local or cloud) where the input data is located.
        output_data (str): Path (local or cloud) where the output data will be located.
    """
    
    # Full set of log-data
    log_data = os.path.join(input_data, "log-data/*/*/*.json")
    df_log = spark.read.json(log_data, schema = sch_log_data)
    
    # Full raw data. Join of song and log datasets based on matching artist 
    df_songs_logs = df_log.\
        join(df_song, df_song.artist_name == df_log.artist, how = "inner")
    
    # users table
    users_table = df_log.select("userId", "firstName", "lastName", "gender", "userAgent")
    
    users_out = os.path.join(output_data, "users.parquet")
    write_to_parquet(dataframe = users_table,\
                     path = users_out,\
                     mode = "overwrite")
    
    # songplays table
    start_time = from_unixtime(df_songs_logs.ts/1000).alias("start_time")
    songplays_table = df_songs_logs.\
        select(start_time, "userId", "level", "sessionId", "location", "userAgent", "artist_id", "song_id").\
        withColumn("year", year(to_timestamp(col("start_time")))).\
        withColumn("month", month(to_timestamp(col("start_time")))).\
        withColumn("songplay_id", monotonically_increasing_id()).\
        where(col("page") == "NextSong")
        
    songplays_out = os.path.join(output_data, "songplays.parquet")
    write_to_parquet(dataframe = songplays_table,\
                     path = songplays_out,\
                     mode = "overwrite",\
                     partition_cols = ["year", "month"])
    
    # time table
    time_table = songplays_table.\
        select("start_time").\
        withColumn("year", year(to_timestamp(col("start_time")))).\
        withColumn("month", month(to_timestamp(col("start_time")))).\
        withColumn("day", dayofmonth(to_timestamp(col("start_time")))).\
        withColumn("week", weekofyear(to_timestamp(col("start_time")))).\
        withColumn("weekday", dayofweek(to_timestamp(col("start_time"))))
        
    time_out = os.path.join(output_data, "time.parquet")
    write_to_parquet(dataframe = time_table,\
                     path = time_out,\
                     mode = "overwrite",\
                     partition_cols = ["year", "month"])
    
    
def main():
    """
    Main function of the etl.py script. It executes the methods process_song_data 
    and process_log_data in serial.
    """
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_SECURITY']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECURITY']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./data/output-data/"
    output_data_s3 = "s3a://juferafo-sparkify-data-lake/"
    
    process_song_data(spark, input_data, output_data_s3)    
    process_log_data(spark, input_data, output_data_s3)

if __name__ == "__main__":
    main()