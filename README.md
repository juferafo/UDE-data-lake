# UDE-data-lake

Data Lakes are widely used by organizations as part of their infrastructure to host both structured (tabular format data like CSVs or JSONS) and non-structured (images, videos, etc...) information. In recent times many companies choose the cloud as the platform where the Data Lake will be maintained since cloud providers (Google, Microsoft or Amazon) offer services to store objects (blobs) in a long-term fashion, with high data availability, fully managed security, high performance, scalability and different storage types for hot/cold data. These features are important to the end user since it makes possible to create a Data Lake with enough resources to suit the needs of the company or, in other words, the problem of under- or over-provissioning of resources is avoided. 

In the use-case presented here a Data Lake will be build on top of [Amazon S3](https://aws.amazon.com/s3/?did=ft_card&trk=ft_card) and a data pipeline will be used to ingest data on the Data Lake. To this end, we will work with two sets of data: the song and log datasets. With the information contained in these datasets we can retrieve valuable insights on the customer's usage of a music app like, for example, what is the top 10 songs played this week? or, how long do customers make use of the paid tier subscription? 

In [this repository](https://github.com/juferafo/UDE-redshift) we explored the same scenario but using a Data Warehouse instead of a Data Lake. This is a valid alternative to the work presented here as we are working with tabular data. Nowadays, the usage of Data Lakes is gaining popularity due to the posibility of storing both structured and un-structured data and fast integration with other cloud services and APIs. 

## Project datasets

As mentioned in the introductions, we are going to process the information present in the song and the log dataset. As described below, they encapsulate different information and, therefore, their schema is distinct.

### Song dataset

The song dataset is a subset of the [Million Song Dataset](http://millionsongdataset.com/) and it contains information about the songs available in the music app. The records are categorized, among other fields, by artist ID, song ID, title, duration, etc... Each row is written as a JSON with the schema shown below and file organized in folders with the following structure.

```
./data/song_data/
└── A
    ├── A
    │   ├── A
    │   ├── B
    │   └── C
    └── B
        ├── A
        ├── B
        └── C
```

Example of a song data file.

```
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```

### Log dataset

The log dataset contains information about the user interaction with the app (sign-in/out, user ID, registration type, listened songs, etc...). This dataset was build from the events simulator [eventsim](https://github.com/Interana/eventsim) and, like the song dataset, the information is stored in JSON files. Below you can find the schema of the log dataset.

```
{
  artist TEXT,
  auth TEXT,
  firstName TEXT,
  gender TEXT,
  itemInSession INT,
  lastName TEXT,
  length DOUBLE,
  level TEXT,
  location TEXT,
  method TEXT,
  page TEXT,
  registration DOUBLE,
  sessionId INT,
  song TEXT,
  status INT,
  ts FLOAT,
  userId INT
}
```

For convenience the above datasets are placed in the S3 buckets showen below. However, it is also possible to employ the `./data` file if we want to work with this data from local.

* Song data bucket: `s3://udacity-dend/song_data`
* Log data bucket: `s3://udacity-dend/log_data`

## Extract-Transform-Load (ETL) pipeline

We are going to make use of an ETL pipeline to process and ingest the song and log data into the Data Lake. ETLs are widely used in the Data Engineering community ant it reffers to a series of steps (Extract, Transform and Load) applied on the raw data. We are going to use [Python](https://www.python.org/download/releases/3.0/) as a programming language and [Apache Spark](https://spark.apache.org/) to benefit from the data management efficiency, task parallelization and easy of use. We must keep in mind that there are a myriad of packages that can be used to implement the ETL like, for example, [Apache Beam](https://beam.apache.org/) or [Apache Hadoop](http://hadoop.apache.org/). Below we describe the AWS resources needed to test this repository, the Data Lake structure and an overview of the ETL pipeline.

### Data Lake structure

The Data Lake will accomodate the song and log data organized in [normalized form](https://en.wikipedia.org/wiki/Database_normalization) with the shape of a [star-schema](https://www.guru99.com/star-snowflake-data-warehousing.html). The star-schemas have a characteristic structure modeled by a single fact table that is connected to the dimension tables via foreign keys. While the fact table contains data related to measurements, metrics or other core aspects of a business process, the dimension tables are used to add descriptive data. In our case the fact table will include data of the songs played by the users during a particular session and the dimension tables will provide additional details on the song, the artist and user details, for example. Below you can find the schema of the fact and dimension tables.

##### `songplays` fact table

```
songplay_id INT,
start_time TIMESTAMP, 
user_id INT, 
level VARCHAR, 
song_id VARCHAR, 
artist_id VARCHAR, 
session_id INT, 
location VARCHAR, 
user_agent VARCHAR
```

##### `artist` dimension table

```
artist_id VARCHAR,
artist_name VARCHAR,
artist_location VARCHAR,
artist_latitude INT,
artist_longitude INT
```

#### `song` dimension table

```
song_id VARCHAR,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration FLOAT
```

##### `time` dimension table

```
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
```

##### `user` dimension table

```
user_id INT,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
```

### S3 bucket creation

We are going to build the Data Lake on top of Amazon S3. In S3 the data is stored inside of [buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html). There are [multiple ways](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) to create an S3 bucket: from the AWS portal, using the AWS SDK or via any of the supported APIs. Below we describe how to create a bucket via the AWS SDK with the command [`mb`] (https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/mb.html) in the `us-west-1` region.

```
$ aws s3 mb s3://<BUCKET_NAME> --region us-west-1
```

When running the above command, take into account that you must create an AWS account and have a valid [AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). Also, keep in mind the [creation rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html) to avoid problems related to naming conventions.

### Pipeline logic

The pipeline code can be found in the script `./etl.py`. The main function of this code performs the following steps:

1. **User authentication**: the AWS access key and the AWS secret key of the user are retrieved from the file `./dl.cfg` and addded as environmental variables. [Here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) you can find more information about credentials authentication with `boto3`.

User authentication coded in `./etl.py`

```python
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_SECURITY']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECURITY']['AWS_SECRET_ACCESS_KEY']
```

Format of `./dl.cfg` for user credentials

```
[AWS_SECURITY]
AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
```

2. **Spark Session**: Definition of the [SparkSession](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html) variable. This parameter is the main entry point of the Apache Spark functionality or, in other words, the variable that we will employ to make use of the Spark SQL and DataFrame features. The Spark Session is instantiated in `./etl.py`.

``` python
    spark = create_spark_session()
```

The method `create_spark_session` can be found in `./lib.py`

``` python
def create_spark_session(log_level = "ERROR"):
    
    [...]
        
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel(log_level)
    
    return spark
```

3. **Song data processing**: the song files are read from their cloud location at `s3://udacity-dend/song-data/*/*/*/*.json` and the data is loaded in memory as a Spark [DataFrame](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html). From this data we can extract the `songs` and `artists` tables and save this information as parquet files in the storage bucket created previously.

4. **Log data processing**: similar to the song data, the log entries are loaded as a Data Frame. At this stage, we can retrieve the `songplays` fact table (by joining the song and log dataframes) and the `time` and `users` tables. Like in the previous step, these tables are saved as parquet files in the storage bucket created previously.

### `./etl.py` usage


## Requirements

1. [`boto3`](https://aws.amazon.com/en/sdk-for-python/)
2. [`Apache Spark`](http://spark.apache.org/docs/2.1.0/api/python/index.html#)
3. [`configparser`](https://docs.python.org/3/library/configparser.html)
