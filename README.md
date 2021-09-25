# UDE-data-lake

Data Lakes are widely used by organizations as part of their infrastructure to maintain data. Frecuently companies build these in the cloud. Several cloud providers (Google, Microsoft or Amazon) offer services to store objects (blobs) in a long-term fashion, with high data availability, security, performance and scalability. These features are important to the end user since they make possible to built a datalake that suits the needs of the company like, for example, include different entry points for cold and hot data. 

In the use-case presented here we are going to build a Data Lake on top of [Amazon S3](https://aws.amazon.com/s3/?did=ft_card&trk=ft_card). Our goal is to store the data contained in the two datasets (song and log datasets) described below. Together, these datasets provide insights on the customer usage of a music application. When the customer plays a song, signs-in or out new records are included in the log dataset. We can use both repositories to answer questions like: what is the top 10 songs played this week? how long do customers make use of the paid tier subscription? 

In [this repository](https://github.com/juferafo/UDE-redshift) we explored the same case scenario but using a Data Warehouse to host the data which is a valid alternative of the use-case presented here. Nowadays, the usage of Data Lakes is gaining popularity due to the posibility of storing both structured and un-structured data and fast connection with other cloud services within the same cloud provider. 

## Project datasets

As mentioned in the introductions, we are going to process the data the song dataset and the log dataset. As described below, these contain different information and, therefore, different schema.

### Song dataset

The song dataset is a subset of the [Million Song Dataset](http://millionsongdataset.com/) and it contains information about the songs available music application. The records of this dataset are categorized, among other fields, by artist ID, song ID, title, duration, etc... Each entry or row is written as a JSON file. The JSONs are organized altogether in folders with the following structure:

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

Below you can find an example of a song data file.

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

The log dataset contains information about the application usage (user ID, registration type, song listened, etc...). It is build from the simulator of events [eventsim](https://github.com/Interana/eventsim) and, like the song dataset, the information is stored in JSON files. Below you can find the schema of the log dataset.

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

For convenience and since this project is focused on cloud technologies the above datasets are placed in the below mentioned S3 buckets. However, it is also possible to employ the `./data` file if we want to retrieve the source data from local.

* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`

## Extract-Transform-Load (ETL) pipeline

We are going to make of of an ETL pipeline to process the song and log data. The term ETL reffers to a series of steps applied on the raw datasets: Extract Transform and Load. This procedure is a common practice in the Data Engineering community. There are a myriad of packages that can be used to program the ETL logic. We are going to use [Python](https://www.python.org/download/releases/3.0/) as a programming language and [Apache Spark](https://spark.apache.org/) to benefit from the data management efficiency, parallelization and easy of use of this framework. 

Below we will describe in details the optional AWS resources needed to test this repository, the Data Lake structure and an overview of the ETL pipeline. 

### Data Lake structure

Our Data Lake will contain the song and log data organized in a [normalized form](https://en.wikipedia.org/wiki/Database_normalization) structure with the shape of a [star-schema](https://www.guru99.com/star-snowflake-data-warehousing.html). The star-schemas are characterized for a fact table that is connected to the dimension tables via foreign keys. The fact table contains data related to measurements, metrics or other core aspects of a business process. In our case, this will be the songs played by the users in a particular session. On the other hand, the dimension tables are used to add descriptive information like, for example, the song, artist or user details.

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

Below you can find how to create an Amazon S3 bucket to be used as Data Lake. There are [multiple ways](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) to create an S3 bucket. Here we describe how to achieve this via the AWS SDK with the command [`mb`] (https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/mb.html)

The below command will create a new S3 bucket with the name `<BUCKET_NAME>` in the `us-west-1` region.

```
$ aws s3 mb s3://<BUCKET_NAME> --region us-west-1
```

Creates a new S3 bucket. To create a bucket, 

When creating a new bucket, take into account that you must create an AWS account and have a valid [AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). Also, keep in mind the [creation rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html) to avoid problems related to naming conventions.

### Pipeline logic

The target is to save the following tables in separate folders within the Data Lake. 

### Pipeline execution



## Requirements

1. [`boto3`](https://aws.amazon.com/en/sdk-for-python/)
2. [`Apache Spark`](http://spark.apache.org/docs/2.1.0/api/python/index.html#)
3. [`configparser`](https://docs.python.org/3/library/configparser.html)
