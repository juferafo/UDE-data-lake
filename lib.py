"""
This script contains helper methods used in the etl.py pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


def create_spark_session():
    """
    This method returns the SparkSession object. It adds the Apache Hadoop AWS library as configuration parameter.
    
    Returns:
        spark (pyspark.sql.session.SparkSession)
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
