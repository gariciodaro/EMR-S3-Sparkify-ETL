# -*- coding: utf-8 -*-
"""
Created on Thu July 30 2020
@author: gari.ciodaro.guerra

etl for sparkify star scheme creation
on parquet files. It can run locally with test
data, or on the cloud with EMR and S3
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import hour, weekofyear, date_format,dayofweek
from pyspark.sql.functions import year,month, dayofmonth
from pyspark.sql.functions import monotonically_increasing_id
#from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql.types import IntegerType, DateType
import argparse

# Get credendials for AWS.
config = configparser.ConfigParser()
#config.read('dl.cfg')
config.read('/home/gari/.aws/credentials')
os.environ['AWS_ACCESS_KEY_ID']=config.get('credentials','KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('credentials','SECRET')


def create_spark_session():
    """Creates SparkSession. General configurarion
    of the script.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Etl process for song_data. Creates tables
     songs and artists.

    Parameters
    ----------
    spark : SparkSession
    input_data : string
        path to song data jsons
    output_data : string
        path to save tables as parquet files.
    """
    song_data = input_data+"/song_data/*/*/*/*.json"
    df = spark.read.json(song_data)
    songs_table = df.select("song_id",
                            "title",
                            "artist_id",
                            "year",
                            "duration").drop_duplicates(subset=['song_id'])

    # Save parquet file songs partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy(
                                    "year",
                                    "artist_id").parquet(output_data+"songs")

    artists_table = df.selectExpr("artist_id",
                                "artist_name as name",
                                "artist_location as location",
                                "artist_latitude as latitude",
                                "artist_longitude as longitude")

    artists_table = artists_table.drop_duplicates(subset=['artist_id'])

    # Save parquet files artists
    artists_table.write.mode("overwrite").parquet(output_data+"artists")


def process_log_data(spark, input_data, output_data, mode):
    """ Etl process for log_data. Creates tables
     users, time and songplays

    Parameters
    ----------
    spark : SparkSession
    input_data : string
        path to song data jsons
    output_data : string
        path to save tables as parquet files.
    mode : string
        aws or local_test.
    """
    if mode=="local_test":
        log_data = input_data+"/log_data/*.json"
    if mode=="aws":
        log_data = input_data+"/log_data/*/*/*.json"

    df =  spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong') 
    users_table =df.selectExpr("userId as user_id",
                                "firstName as first_name",
                                "lastName as last_name",
                                "gender",
                                "level").drop_duplicates(subset=['user_id'])

    # Save parquet files users
    users_table.write.mode("overwrite").parquet(output_data+"users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x),IntegerType())
    df = df.withColumn("start_time",
                get_timestamp(col("ts")/1000.0)).drop_duplicates(subset=['ts'])

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000),DateType())
    df =  df.withColumn("datetime", get_datetime(col("ts")))

    time_table = df.select("start_time","datetime")

    dict_time_func={"hour":hour,
                "day":dayofmonth,
                "week":weekofyear,
                "month":month,
                "year":year ,
                "weekday":dayofweek}

    for key,value in dict_time_func.items():
        time_table = time_table.withColumn(key,value("datetime"))

    # Save parquet files time   
    time_table.write.mode("overwrite").partitionBy(
                                        "year",
                                        "month").parquet(output_data+"time")

    song_table_read    = spark.read.parquet(
                            output_data+"songs")["song_id",
                                                "title",
                                                "artist_id",
                                                "duration"]
    artists_table_read = spark.read.parquet(
                            output_data+"artists")["artist_id",
                                                "name"]
    df_song_comp = song_table_read.join(artists_table_read, 
                                        on=['artist_id'],
                                        how='left')
    # extract columns from joined song and log datasets to create songplays table 
    songplays_aux_table = df.selectExpr(
            "song as title",
            "artist as name",
            "length as duration",
            "start_time",
            "userId as user_id",
            "level",
            "sessionId as session_id",
            "location",
            "userAgent as user_agent").drop_duplicates(subset=['start_time'])

    songplays_aux_table = songplays_aux_table.withColumn("songplay_id", 
                                                monotonically_increasing_id())

    songplays_table = songplays_aux_table.join(df_song_comp, 
                                            on=['title','name','duration'],
                                            how='left')

    songplays_table = songplays_table.selectExpr('songplay_id',
                                                'start_time',
                                                'user_id',
                                                'level',
                                                'song_id',
                                                'artist_id',
                                                'session_id',
                                                'location',
                                                'user_agent')
    # Save parquet files songplays 
    songplays_table.write.mode("overwrite").parquet(output_data+"songplays")

def main(mode):
    """ main controller of etl
    Parameters
    ----------
    mode : string
        aws or local_test. Use local_test for test with small data set.
    """
    spark = create_spark_session()
    if mode=="local_test":
        input_data='/home/gari/Desktop/myGItRepos/project_spark/data'
        output_data='./parquet_area/'
    if mode=="aws":
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://projectsparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, mode)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="""local_test or aws.
                                         if aws read and write data to S3. 
                                         If local_test, smaller dataset, 
                                         execute spark locally""")
    args = parser.parse_args()

    main(args.mode)
