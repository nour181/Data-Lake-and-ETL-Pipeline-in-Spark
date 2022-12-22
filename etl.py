import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Description:
        Process songs data files
        Create song table and artist table
        
    
    Args:
        songs table: table contains data about songs.
        artists tables: table contains data about artists.
        spark: spark session
        input_data: the input data path
        output_data: the output data path
    Returns:
        Create song table and artist table
    
    
    
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data, columnNameOfCorruptRecord = "corrupt_record").drop_duplicates()
    
    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data, "songs/",mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data,"artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):

    
    """
    Description:
        Process log data files
        Create time, user, songplays tables
        
    
    Args:
        time table: table contains data about time(hour, day, week, day of month, week of year, day of week, year).
        user tables: table contains data about artists.
        songplays tables: table contains data about artists.
        spark: spark session
        input_data: the input data path
        output_data: the output data path
    Returns:
        Create time, user, songplays tables
    
    
    
    """    
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log-data/")

    # read log data file
    df = spark.read.json(log_data, columnRecordOrCorrupt = "corrupt_record").drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextPage")

    # extract columns for users table    
    users_table = df.select("user_id","first_name","last_name", "gender","level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data,"users/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(int(x)/1000.0))
    df = df.withColumn("start_time", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time"))\
                   .withColumn("day", dayofmonth("start_time"))\
                   .withColumn("week", weekofyear("start_time"))\
                   .withColumn("month", month("start_time"))\
                   .withColumn("year", year("start_time"))\
                   .withColumn("weekday", dayofweek("start_time"))\
                   .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode="overwrite", partitionBy = ["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read\
                        .format("parquet")\
                        .option("basePath", os.path.join(output_data, "songs/"))\
                        .load(os.path.join(output_data, "songs/*/*/*"))

    # extract columns from joined song and log datasets to create songplays table 
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name))
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.ts, 'left'
    ).drop(artists_songs_logs.year)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
