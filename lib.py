"""
This script contains helper methods used in the etl.py pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DecimalType, DoubleType, IntegerType, LongType

def create_spark_session(log_level = "ERROR"):
    """
    This method returns the SparkSession object. It adds the Apache Hadoop AWS library as configuration parameter. 
    
    Args:
        log_level (str, optional): This parameter allows to control the logLevel details.
        pyspark defaults to WARN but this method modifies this to ERROR. 
            Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    
    Returns:
        spark (pyspark.sql.session.SparkSessiony): SparkSession object
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel(log_level)
    
    return spark


def write_to_parquet(dataframe, path, mode = "errorifexists", partition_cols = None):
    """
    This method can be used to write dataframes as parquet files specifying the output path (local), 
    the mode and the partition columns
    
    Arguments:
        dataframe (pyspark.sql.dataframe.DataFrame): Spark dataframe
        path (str): path where the dataframe will be saved in parquet format (local or cloud)
        mode (str, optional): action taken if the file already exists. 
            Valid values: append, overwrite, ignore, error or errorifexists
        partition_cols (str, optional): colums used to partition the data
    """
    
    dataframe.write.parquet(path, mode, partition_cols)
    print("\nDataframe data saved to {0}".format(path))
    print("Parquet file partitioned by {0}\n".format(partition_cols))
    

# Schema of the song dataframe
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


# Schema of the log dataframe
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
