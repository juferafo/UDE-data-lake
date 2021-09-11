# TODO LIST
# Create an S3 bucket -> include this in the README.md
# Write data in different folders in S3
# Test this code with EMR

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, from_unixtime, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DecimalType, DoubleType, IntegerType, LongType
from lib import create_spark_session, write_to_parquet

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_SECURITY']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECURITY']['AWS_SECRET_ACCESS_KEY']


def process_song_data(spark, input_data, output_data):
    
    # Full set of song data
    #song_data = os.path.join(input_data, "*/*/*/*.json")
    
    # Subset A/A/A/*.json of song data
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    
    # Schema song-data
    sch_song_data = StructType([ \
        StructField("artist_id", StringType(), True), \
        StructField("artist_latitude", DecimalType(),True), \
        StructField("artist_location", StringType(), True), \
        StructField("artist_longitude", DecimalType(), True), \
        StructField("artist_name", StringType(), True), \
        StructField("duration", DoubleType(), True), \
        StructField("num_songs", IntegerType(), True), \
        StructField("song_id", StringType(), True), \
        StructField("title", StringType(), True), \
        StructField("year", IntegerType(), True) \
      ])

    df_song = spark.read.json(song_data, schema = sch_song_data)

    # extract columns to create songs table
    songs_table = df_song.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    write_to_parquet(dataframe = songs_table,\
                     path = "./data/output-data/songs.parquet",\
                     mode = "overwrite",\
                     partition_cols = ["year", "artist_id"])
    
    
    # extract columns to create artists table
    artists_table = df_song.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table = write_to_parquet(dataframe = artists_table,\
                                     path = "./data/output-data/artists.parquet",\
                                     mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    
    """# get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table"""
    
    return None


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./data/output-data/"
    
    process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
