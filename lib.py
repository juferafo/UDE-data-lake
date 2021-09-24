"""
This script contains helper methods used in the etl.py pipeline
"""

from pyspark.sql import SparkSession
#from pyspark.sql.functions import udf, col
#from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DecimalType, DoubleType, IntegerType, LongType

def create_spark_session():
    """
    This method returns the SparkSession object. It adds the Apache Hadoop AWS library as configuration parameter.
    
    Returns:
        spark (pyspark.sql.session.SparkSession): SparkSession object
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def write_to_parquet(dataframe, path, mode = "errorifexists", partition_cols = None):
    """
    This method can be used to write dataframes as parquet files specifying the output path (local), 
    the mode and the partition columns
    
    Arguments:
        dataframe:
        path (str):
        mode (str, optional):
        partition_cols (str, optional):
    """
    
    dataframe.write.parquet(path, mode, partition_cols)
    

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


# Schema log-data
sch_log_data = StructType([ \
                           StructField("artist", StringType(), True), \
                           StructField("auth", StringType(),True), \
                           StructField("firstName", StringType(), True), \
                           StructField("gender", StringType(), True), \
                           StructField("itemInSession", LongType(), True), \
                           StructField("lastName", StringType(), True), \
                           StructField("length", DoubleType(), True), \
                           StructField("level", StringType(), True), \
                           StructField("location", StringType(), True), \
                           StructField("method", StringType(), True), \
                           StructField("page", StringType(), True), \
                           StructField("registration", DoubleType(), True), \
                           StructField("sessionId", LongType(), True), \
                           StructField("song", StringType(), True), \
                           StructField("status", StringType(), True), \
                           StructField("ts", DoubleType(), True), \
                           StructField("userAgent", StringType(), True), \
                           StructField("userId", StringType(), True) \
                          ])
