# Sparkify data base with EMR(Spark) and S3.

-Note this is a hypothetical enterprise that was built for the Nano Degree in 
Data Engineering. Udacity.

## Project: Data Lake
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Objetive: build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

Please read read the following repositories to get context:
+ [postgresql star scheme.](https://github.com/gariciodaro/postgresql-Sparkify-data-modeling)
+ [cassandra query optimized.](http://garisplace.com/udacity/cassandra_project.html)
+ [Redshift Data warehouse.](https://github.com/gariciodaro/redshift-Sparkify-data-warehouse)

<div>
<img src="./work_slow.svg">
</div>


## Folder Structure

```
├── awsUtils
│   ├── createAwsBucket.py
│   ├── createEMR.py
│   └── README.MD
├── data
│   ├── log_data
│   │   ├── 2018-11-01-events.json
│   │   ├── 2018-11-02-events.json ...
│   │
│   └── song_data
│       └── A
│           ├── A
│           │   ├── A
│           │   │   ├── TRAAAAW128F429D538.json
│           │   │   ├── TRAAABD128F429CF47.json
│           │   │   ├── TRAAADZ128F9348C2E.json
│           │   │   ├── TRAAAEF128F4273421.json ...
├── dev_etl.ipynb
├── dl.cfg
├── etl.py
├── experiments_spark.ipynb
├── parquet_area
│   ├── artists
│   │   ├── part-00000-aca38d87-92aa-4f0e-88e9-ca0f4d5f3f63-c000.snappy.parquet
│   │   ├── part-00002-aca38d87-92aa-4f0e-88e9-ca0f4d5f3f63-c000.snappy.parquet
│   │   ├── part-00003-aca38d87-92aa-4f0e-88e9-ca0f4d5f3f63-c000.snappy.parquet...
│   ├── songplays
│   │   ├── part-00000-77b7570f-0cbb-497b-afe9-2ff42772e477-c000.snappy.parquet
│   │   ├── part-00001-77b7570f-0cbb-497b-afe9-2ff42772e477-c000.snappy.parquet...
│   ├── songs
│   │   ├── _SUCCESS
│   │   ├── year=0
│   │   │   ├── artist_id=AR051KA1187B98B2FF
│   │   │   │   └── part-00171-406ff7d5-60fa-44be-98fd-d70de15be39b.c000.snappy.parquet
│   │   │   ├── artist_id=AR10USD1187B99F3F1
│   │   │   │   └── part-00114-406ff7d5-60fa-44be-98fd-d70de15be39b.c000.snappy.parquet...
│   ├── time
│   │   ├── _SUCCESS
│   │   └── year=2018
│   │       └── month=11
│   │           ├── part-00000-84e7cc1c-0ea0-463b-87fe-2f68a6006e81.c000.snappy.parquet
│   │           ├── part-00001-84e7cc1c-0ea0-463b-87fe-2f68a6006e81.c000.snappy.parquet
│   │           ├── part-00002-84e7cc1c-0ea0-463b-87fe-2f68a6006e81.c000.snappy.parquet...
│   └── users
│       ├── part-00000-bdaa7028-250f-40d6-92ff-ba26be165a73-c000.snappy.parquet
│       ├── part-00003-bdaa7028-250f-40d6-92ff-ba26be165a73-c000.snappy.parquet
│       ├── part-00007-bdaa7028-250f-40d6-92ff-ba26be165a73-c000.snappy.parquet...
├── README.md
└── work_slow.svg
```

## Files
+ ```etl.py```: main pyspark script to do the ETL.
+ ```awsUtils/*```: Set of scripts to create buckets and start an EMR cluster. 
Please see the readme inside for more information.
+ ```data/*```: data used for local testing.
+ ```dev_etl.ipynb``` : Development notebook. Use for step by step explorarion.
+ ```dl.cfg```: file that can hold AWS credentials. Notice that I prefered to call
my credentials from my root folder instead.
+ ```experiments_spark.ipynb``` pyspark dataframe explorarion.
+ ```parquet_area/*```: stored parquet files after local testing execution.

## Usage
+ There are two modes of operation. local (local_test) or cloud(aws)
    + **local_test** will use ```data/*``` files to make an ETL locally and store the
        tables in ```parquet_area/*```
    + To run type in terminal```python ./etl.py --mode local_test```
    + **aws** will use s3 urls to read udacity bucket and then will write the result
    into a the s3 bucket specified in the main function of ```etl.py```.
    + To run, SSH to EMR instance, copy ```etl.py``` and
     execute the script ```/usr/bin/spark-submit --master yarn ./etl.py --mode aws```.
     Make sure the credentials are properly set.