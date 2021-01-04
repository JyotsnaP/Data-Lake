import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.functions import from_unixtime,udf,dayofmonth,weekofyear, col
from pyspark.sql.functions import monotonically_increasing_id, year, month
from pyspark.sql.functions import  dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql.types import IntegerType, DateType,TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    song_schema = StructType([
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_name",StringType()),
        StructField("duration",DoubleType()),
        StructField("num_songs",IntegerType()),
        StructField("title",StringType()),
        StructField("year",IntegerType()),
    ])

    # read song data file
    df = spark.read.json(song_data ,schema=song_schema)
    
    # extract columns to create songs table
    song_columns = ["title", "artist_id","year", "duration"]
    songs_table = df.select(song_columns).dropDuplicates().withColumn("song_id",
    monotonically_increasing_id())
    songs_table.show(songs_table.count())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')
    
    # extract columns to create artists table
    artists_columns = ["artist_id", "artist_name", "artist_location","artist_latitude",
    "artist_longitude"]

    artists_table = df.select(artists_columns) \
    .withColumnRenamed("artist_name","name") \
    .withColumnRenamed("artist_location","location") \
    .withColumnRenamed("artist_latitude","latitude") \
    .withColumnRenamed("artist_longitude","longitude") \
    .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')

def process_log_data(spark, input_data, output_data):

    # get filepath to log data file
    log_data = input_data+"log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_columns = ["userId", "firstName", "lastName", "gender", "level"]
    users_table = df.select(users_columns) \
    .withColumnRenamed("userId","user_id") \
    .withColumnRenamed("firstName","first_name") \
    .withColumnRenamed("lastName","last_name") \
    .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')


    # create timestamp column from original timestamp column
    df = df.select("ts").withColumn("start_time",from_unixtime(F.col('ts')/1000))
     
    # extract columns to create time table
    time_table = df.select("ts","start_time").dropDuplicates() \
    .withColumn("hour", hour(col("start_time"))) \
    .withColumn("day", dayofmonth(col("start_time"))) \
    .withColumn("week", weekofyear(col("start_time"))) \
    .withColumn("month", month(col("start_time"))) \
    .withColumn("year", year(col("start_time"))) \
    .withColumn("weekday", date_format(col("start_time"), 'E'))

    time_table=time_table.dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')


    # read in log data & song data for songplays_table 
    log_data = input_data+"log_data/*.json"
    df = spark.read.json(log_data)
    df_log = df.filter(df.page == 'NextSong')
    song_data = input_data+"song_data/*/*/*/*.json"
    song_schema = StructType([
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_name",StringType()),
        StructField("duration",DoubleType()),
        StructField("num_songs",IntegerType()),
        StructField("title",StringType()),
        StructField("year",IntegerType())
    ])
    df_songs = spark.read.json(song_data,schema=song_schema)
    df_songs = df_songs.withColumn("song_id", monotonically_increasing_id())
    test =  df_log.alias('log') \
    .join(df_songs.alias('songs'),col('log.song') == col('songs.title'))
    test1 = test.filter(test.artist == test.artist_name)
    test1 = test1.alias('test1').join(time_table.alias('time_table'), \
        col('test1.ts') == col('time_table.ts')) \
        .select([col('test1.artist'),col('test1.auth'), \
        col('test1.firstName'),col('test1.itemInSession'), \
        col('test1.lastName'),col('test1.level'),  \
        col('test1.location'),col('test1.method'),  \
        col('test1.page'),col('test1.registration'), \
        col('test1.sessionId'),col('test1.song'), \
        col('test1.ts'),col('test1.userAgent'), \
        col('test1.userId'),col('test1.artist_id'), \
        col('test1.artist_latitude'),col('test1.artist_location'),  \
        col('test1.artist_longitude'),col('test1.artist_name'), \
        col('test1.duration'),col('test1.num_songs'),col('test1.duration'), \
        col('test1.year'),col('test1.song_id'),col('time_table.ts'), \
        col('time_table.start_time'),col('time_table.hour'), \
        col('time_table.day'),col('time_table.week'),col('time_table.month'), \
        col('time_table.year'),col('time_table.weekday')])

    # extract columns from joined song and log datasets to create songplays table
    songplays_table_columns = ["time_table.ts","userId","level","song_id",
    "artist_id","sessionId","location","userAgent","time_table.year",
    "time_table.month"]
    songplays_table = test1.select(songplays_table_columns) \
    .withColumnRenamed("ts","start_time.ts") \
    .withColumnRenamed("userId","user_id") \
    .withColumnRenamed("sessionId","session_id") \
    .withColumnRenamed("userAgent","user_agent") \
    .dropDuplicates().repartition("year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month") \
    .parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    
    input_data = "s3://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()