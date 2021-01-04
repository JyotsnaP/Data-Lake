# Project Data-Lake
___
## Project description
___

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The aim of this project is to build an ETL pipeline using Spark and Data Lakes for data hosted in S3. The ETL will mainly load data from S3,  process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.


## Project description
___
This project includes the following files:
- etl.py  - This is the file iin which data is read from S3, processed and written back to S3. 
- README.md  - This has details on everything about the project
- dl.cfg - 


#### To be able to access S3 data from your local project workspace:
___ 
You should be able to find the AWS access and secret keys in the IAM console. 

```
aws configure
> AWS Access Key ID : <enter your access key>
> AWS Secret Access Key : <enter your secret access key>
> Default region name : us-west-2
> Default output format : <enter>
```



#### Get an idea of data in the S3 location by:
___
```
aws s3 ls s3://udacity-dend/ 
```

RESPONSE:
```
                           PRE data-pipelines/
                           PRE log-data/
                           PRE log_data/
                           PRE pagila/
                           PRE song-data/
                           PRE song_data/
                           PRE udac-data-pipelines/
2019-04-02 16:58:44        456 log_json_path.json
```

#### We are primarily interested in song-data and log-data
___

### SONG DATA
	```
	aws s3 ls s3://udacity-dend/song-data/
	```

	RESPONSE:
	```
	                           PRE A/
	                           PRE B/
	                           PRE C/
	                           PRE D/
	                           PRE E/
	                           PRE F/
	                           PRE G/
	                           PRE H/
	                           PRE I/
	                           PRE J/
	                           PRE K/
	                           PRE L/
	                           PRE M/
	                           PRE N/
	                           PRE O/
	                           PRE P/
	                           PRE Q/
	                           PRE R/
	                           PRE S/
	                           PRE T/
	                           PRE U/
	                           PRE V/
	                           PRE W/
	                           PRE X/
	                           PRE Y/
	                           PRE Z/
	2019-04-05 00:09:20          0 
	```

### LOG DATA
	```
	aws s3 ls s3://udacity-dend/log-data/
	```


	```
	    PRE 2018/
		2019-04-07 03:19:23          0 
	```

#### Get a sample of how one file looks like by:
___
### SONG DATA: 

### LOG DATA: 
	```{
		"artist":"Survivor",
		"auth":"Logged In",
		"firstName":"Jayden",
		"gender":"M",
		"itemInSession":0,
		"lastName":"Fox",
		"length":245.36771,
		"level":"free",
		"location":"New Orleans-Metairie, LA",
		"method":"PUT",
		"page":"NextSong",
		"registration":1541033612796.0,
		"sessionId":100,
		"song":"Eye Of The Tiger",
		"status":200,"ts":1541110994796,
		"userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"101"
	}```


#### SAMPLE : 
___
	{
		"song_id": "SOBLFFE12AF72AA5BA", 
		"num_songs": 1, 
		"title": "Scream", 
		"artist_name": "Adelitas Way", 
		"artist_latitude": null, 
		"year": 2009, 
		"duration": 213.9424, 
		"artist_id": "ARJNIUY12298900C91", 
		"artist_longitude": null, 
		"artist_location": ""
	}


#### Building/Testing the ETL for small data of song-data and log-data dataset using `pyspark` console: 
___

1. Unzip(unsing unzip command) the data from the /data folder and create two new folders under /data with the respective data:
		- /data/log_data
		- /data/song_data
2. Log into pyspark 
```
	pyspark
```

3. When the console is open do the following:
	- Import all. the required libraries	
	- Read log and song data json into a dataframe and print it: 
```
>>> import configparser
>>> from datetime import datetime
>>> import os
>>> from pyspark.sql import SparkSession
>>> from pyspark.sql.functions import udf, col, monotonically_increasing_id
>>> from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
>>> from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType,TimestampType

# Read the json
>>> df = spark.read.json("/home/workspace/data/song_data/A/A/A/TRAAAAW128F429D538.json") 

# Print the dataframe
>>> df.show()
+------------------+---------------+---------------+----------------+-----------+---------+---------+------------------+----------------+----+
|         artist_id|artist_latitude|artist_location|artist_longitude|artist_name| duration|num_songs|           song_id|           title|year|
+------------------+---------------+---------------+----------------+-----------+---------+---------+------------------+----------------+----+
|ARD7TVE1187B99BFB1|           null|California - LA|            null|     Casual|218.93179|        1|SOMZWCG12A8C13C480|I Didn't Mean To|   0|
+------------------+---------------+---------------+----------------+-----------+---------+---------+------------------+----------------+----+
```

4. Now we will print only the fields we are interested in using a defined schema: 
```
# Define the schema
>>> song_schema = StructType([
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

# Read the json
>>> df = spark.read.json("/home/workspace/data/song_data/A/A/A/TRAAAAW128F429D538.json",schema=song_schema)

# Print the dataframe
>>> df.show()
+------------------+---------------+---------------+----------------+-----------+---------+---------+----------------+----+
|         artist_id|artist_latitude|artist_location|artist_longitude|artist_name| duration|num_songs|           title|year|
+------------------+---------------+---------------+----------------+-----------+---------+---------+----------------+----+
|ARD7TVE1187B99BFB1|           null|California - LA|            null|     Casual|218.93179|        1|I Didn't Mean To|   0|
+------------------+---------------+---------------+----------------+-----------+---------+---------+----------------+----+

```

5. The same thing is repeated for log data: 
```
# Read the json
df = spark.read.json("/home/workspace/data/log_data/2018-11-01-events.json")

# Print the dataframe
>>> df.show(5)
+-------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+------------+------+-------------+--------------------+------+
| artist|     auth|firstName|gender|itemInSession|lastName|   length|level|            location|method|    page|     registration|sessionId|        song|status|           ts|           userAgent|userId|
+-------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+------------+------+-------------+--------------------+------+
|   null|Logged In|   Walter|     M|            0|    Frye|     null| free|San Francisco-Oak...|   GET|    Home|1.540919166796E12|       38|        null|   200|1541105830796|"Mozilla/5.0 (Mac...|    39|
|   null|Logged In|   Kaylee|     F|            0| Summers|     null| free|Phoenix-Mesa-Scot...|   GET|    Home|1.540344794796E12|      139|        null|   200|1541106106796|"Mozilla/5.0 (Win...|     8|
|Des'ree|Logged In|   Kaylee|     F|            1| Summers|246.30812| free|Phoenix-Mesa-Scot...|   PUT|NextSong|1.540344794796E12|      139|You Gotta Be|   200|1541106106796|"Mozilla/5.0 (Win...|     8|
|   null|Logged In|   Kaylee|     F|            2| Summers|     null| free|Phoenix-Mesa-Scot...|   GET| Upgrade|1.540344794796E12|      139|        null|   200|1541106132796|"Mozilla/5.0 (Win...|     8|
|Mr Oizo|Logged In|   Kaylee|     F|            3| Summers|144.03873| free|Phoenix-Mesa-Scot...|   PUT|NextSong|1.540344794796E12|      139|     Flat 55|   200|1541106352796|"Mozilla/5.0 (Win...|     8|
+-------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+------------+------+-------------+--------------------+------+

```


6. Now to continue testing on local, set the `output_folder="output_data = "/home/workspace/output/""` and write the song_table in parquet format - where we have partitioned the df first by year, and then by artist_id. 


```
songs_table = df.select(song_columns).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')
```

NOTE:  monotonically_increasing_id - A column that generates monotonically increasing 64-bit integers. The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive

Once that is done you should be able to see your output folder being created in your workspace like this:

![sc-1](https://github.com/JyotsnaP/Data-Lake/blob/master/images/sc-1.png)

7. Repeat the above steps for user table, and time table

#### RESULTS : 
```
>>> df = spark.read.parquet("/home/workspace/output/songs/")
>>> df.count()
71
```

```
>>> df = spark.read.parquet("/home/workspace/output/artists/")
>>> df.count()
69
```

```
>>> df = spark.read.parquet("/home/workspace/output/users/")
>>> df.count()
104
```

```
>>> df = spark.read.parquet("/home/workspace/output/time")
>>> df.show()
+-------------+-------------------+----+---+----+-------+----+-----+
|           ts|         start_time|hour|day|week|weekday|year|month|
+-------------+-------------------+----+---+----+-------+----+-----+
|1542294199796|2018-11-15 15:03:19|  15| 15|  46|    Thu|2018|   11|
|1542302745796|2018-11-15 17:25:45|  17| 15|  46|    Thu|2018|   11|
|1542319191796|2018-11-15 21:59:51|  21| 15|  46|    Thu|2018|   11|
|1542187376796|2018-11-14 09:22:56|   9| 14|  46|    Wed|2018|   11|
|1542217734796|2018-11-14 17:48:54|  17| 14|  46|    Wed|2018|   11|
|1543398807796|2018-11-28 09:53:27|   9| 28|  48|    Wed|2018|   11|
|1543444194796|2018-11-28 22:29:54|  22| 28|  48|    Wed|2018|   11|
|1541414107796|2018-11-05 10:35:07|  10|  5|  45|    Mon|2018|   11|
|1541421833796|2018-11-05 12:43:53|  12|  5|  45|    Mon|2018|   11|
|1541440472796|2018-11-05 17:54:32|  17|  5|  45|    Mon|2018|   11|
|1542097517796|2018-11-13 08:25:17|   8| 13|  46|    Tue|2018|   11|
|1542128532796|2018-11-13 17:02:12|  17| 13|  46|    Tue|2018|   11|
|1542143755796|2018-11-13 21:15:55|  21| 13|  46|    Tue|2018|   11|
|1543556584796|2018-11-30 05:43:04|   5| 30|  48|    Fri|2018|   11|
|1543556636796|2018-11-30 05:43:56|   5| 30|  48|    Fri|2018|   11|
|1543558077796|2018-11-30 06:07:57|   6| 30|  48|    Fri|2018|   11|
|1543578063796|2018-11-30 11:41:03|  11| 30|  48|    Fri|2018|   11|
|1543594388796|2018-11-30 16:13:08|  16| 30|  48|    Fri|2018|   11|
|1542393049796|2018-11-16 18:30:49|  18| 16|  46|    Fri|2018|   11|
|1542393659796|2018-11-16 18:40:59|  18| 16|  46|    Fri|2018|   11|
+-------------+-------------------+----+---+----+-------+----+-----+
only showing top 20 rows

>>> df.count()
6813      
```

```                                                                                                        
>>> df = spark.read.parquet("/home/workspace/output/songplays")
>>> df.count()
1
>>> df.show()
+-------------+-------+-----+----------+------------------+----------+--------------------+--------------------+----+-----+
|start_time.ts|user_id|level|   song_id|         artist_id|session_id|            location|          user_agent|year|month|
+-------------+-------+-----+----------+------------------+----------+--------------------+--------------------+----+-----+
|1542837407796|     15| paid|8589934607|AR5KOSW1187FB35FF4|       818|Chicago-Napervill...|"Mozilla/5.0 (X11...|2018|   11|
+-------------+-------+-----+----------+------------------+----------+--------------------+--------------------+----+-----+

```

