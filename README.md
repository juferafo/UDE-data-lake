# UDE-data-lake

Data Lakes are widely used by organizations as part of their infrastructure to maintain data. Frecuently companies build these in the cloud. Several cloud providers (Google, Microsoft or Amazon) offer services to store objects (blobs) in a long-term fashion, with high data availability, security, performance and scalability. These features are important to the end user since they make possible to built a datalake that suits the needs of the company like, for example, include different entry points for cold and hot data. 

In the use-case presented here we are going to build a Data Lake on top of [Amazon S3](https://aws.amazon.com/s3/?did=ft_card&trk=ft_card). Our goal is to store the data contained in the two datasets (song and log datasets) described below. Together, these datasets provide insights on the customer usage of a music application. When the customer plays a song, signs-in or out new records are included in the log dataset. We can use both repositories to answer questions like: what is the top 10 songs played this week? how long do customers make use of the paid tier subscription? 

In [this repository](https://github.com/juferafo/UDE-redshift) we explored the same case scenario but using a Data Warehouse to host the data which is a valid alternative of the use-case presented here. Nowadays, the usage of Data Lakes is gaining popularity due to the posibility of storing both structured and un-structured data and fast connection with other cloud services within the same cloud provider. 

### Project datasets

### Deploying AWS resources

### Data Lake structure

### Extract-Transform-Load (ETL) pipeline
