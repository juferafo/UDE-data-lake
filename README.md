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


### Deploying AWS resources

### Data Lake structure

### Pipeline execution

## Requirements

1. [`boto3`](https://aws.amazon.com/en/sdk-for-python/)
2. [`Apache Spark`](http://spark.apache.org/docs/2.1.0/api/python/index.html#)
3. [`configparser`](https://docs.python.org/3/library/configparser.html)
