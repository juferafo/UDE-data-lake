import configparser
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, from_unixtime, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from lib import create_spark_session
from lib import sch_song_data, sch_log_data
from lib import write_to_parquet
from datetime import datetime


def process_song_data(spark, input_data, output_data):
    """
    This method process the song dataset and extracts the fields corresponding to the 
    songs and artists tables. It writes these tables in parquet format in 
    the output path provided in the argument "output_data"
    
    Args:
        spark (pyspark.sql.session.SparkSession): SparkSession object
        input_data (str): Path (local or cloud) where the input data is located.
        output_data (str): Path (local or cloud) where the output data will be located.
    """
    
    global df_song, song_data
    
    # Song data
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    df_song = spark.read.json(song_data, schema = sch_song_data)
    df_song.dropna(how='all')

    # songs table
    print("Processing songs table")
    songs_table = df_song.select("song_id", "title", "artist_id", "year", "duration")
    songs_table.printSchema()
    
    # songs table written to parquet file partitioned by year and artist
    songs_out = os.path.join(output_data, "songs.parquet")
    write_to_parquet(dataframe = songs_table,\
                     path = songs_out,\
                     mode = "overwrite",\
                     partition_cols = ["year", "artist_id"])
    
    # artists table
    print("Processing artists table")
    artists_table = df_song.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    artists_table.printSchema()
    
    # artists table written to parquet files
    artists_out  = os.path.join(output_data, "artists.parquet")
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
    
    # Song data
    log_data = os.path.join(input_data, "log-data/*/*/*.json")
    df_log = spark.read.json(log_data, schema = sch_log_data)
    df_log.dropna(how='all')
    
    # Full raw data. Join of song and log datasets based on matching artist
    # This will be usefull later on the songplays table
    df_songs_logs = df_log.\
        join(df_song, df_song.artist_name == df_log.artist, how = "inner")
    
    # users table
    users_table = df_log.select("userId", "firstName", "lastName", "gender", "userAgent")
    
    # users table written to parquet
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
    
    # songplays table written to parquet file partitioned by year and month
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
    
    # time table written to parquet file partitioned by year and month
    time_out = os.path.join(output_data, "time.parquet")
    write_to_parquet(dataframe = time_table,\
                     path = time_out,\
                     mode = "overwrite",\
                     partition_cols = ["year", "month"])
    
    
def main():
    """
    Main function of the etl.py script. It executes the methods process_song_data 
    and process_log_data in serial. This script can be executed with the following flags 
    that allow to specify the route of the input and output data. This works both for 
    local and cloud (tested on S3):
    
        --input_data (str):  Path of the input data where the log and song datasets are located.
        --output_data (str): Path of the output data where the parquet files will be written.
    
    Example:
    
        $python etl.py --input_data <PATH_TO_INPUT_DATA> --output_data <PATH_TO_OUTPUT_DATA>
    """
    
    # Initialization of arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(\
            "--input_data",\
            default=None,\
            help="Path of the input data where the log and song datasets are located.",\
            type=str)

    parser.add_argument(\
            "--output_data",\
            default=None,\
            help="Path of the output data where the parquet files will be written.",\
            type=str)

    args = parser.parse_args()
    input_path  = args.input_data
    output_path = args.output_data
    
    if not input_path:
        print("No input data path provided. Using the default value s3a://udacity-dend/\n")
        input_path = "s3a://udacity-dend/"
    
    if not output_path:
        print("No output data path provided. Using the default value ./sparkyfy-output-data\n")
        output_path = "sparkyfy-output-data"
        if not os.path.isdir(output_path):
            print("Directory ./sparkyfy-output-data not found")
            print("Creating ./sparkyfy-output-data\n")
            os.makedirs(output_path)
    
    # Reading AWS configuration
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_SECURITY']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECURITY']['AWS_SECRET_ACCESS_KEY']

    # Creating the SparkSession
    spark = create_spark_session()
    
    # For testing/dev purposes
    # output_data_local = "./data/output-data/"
    # output_data_s3 = "s3a://juferafo-sparkify-data-lake/"
    
    # Extraction and transformation of the song and log data
    print("Processing Song dataset\n")
    process_song_data(spark, input_path, output_path)    
    print("Processing Log dataset\n")
    process_log_data(spark, input_path, output_path)
    
    
if __name__ == "__main__":
    main()