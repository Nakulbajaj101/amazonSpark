from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, date_format, dayofmonth, hour, lit,
                                   month, row_number, to_timestamp, udf,
                                   weekofyear, year)
from pyspark.sql.window import Window


def create_spark_session():
    """
    Function to create a spark session with hadoop 2.7.0 aws 
    configuration under the hood
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function to read data from song_data directory and 
    process that data using spark which is then written
    to parquet files of artists and songs to a desired location
    """

    # get filepath to song data file
    song_data = input_data + "song_data"
    
    # read song data file
    df = spark.read.json(song_data + "/*/*/*/*.json")

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id")\
    .parquet(output_data + "songs",'overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id",\
                                  "artist_name as name",\
                                  "artist_location as location",\
                                  "artist_latitude as latitude",\
                                  "artist_longitude as longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists",'overwrite')
    
    
def process_log_data(spark, input_data, output_data):
    """
    Function to read in songs data from song_data directory
    and log_data directory and processes data using spark to produce users,
    time and songplays (joining songs and logs data) and writing to 
    parquet files in desired location
    """

    # get filepath to log data file
    log_data = input_data + "log_data"
    w = Window().orderBy(lit('A'))

    # read log data file
    df = spark.read.json(log_data + "/*/*/*.json")
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")\
         .withColumn("songplay_id", row_number().over(w))

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id",\
                                "firstName as first_name",\
                                "lastName as last_name",\
                                "gender",\
                                "level").dropDuplicates()
    
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users",'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.utcfromtimestamp(ts/1000.0)\
                                   .strftime('%Y-%m-%d %H:%M:%S'))
    
    df = df.withColumn('start_time',get_timestamp('ts'))
    
    # create datetime column from original timestamp column

    time_table = df.select("start_time").dropDuplicates()\
                   .withColumn("start_time",\
                               to_timestamp("start_time","yyyy-MM-dd HH:mm:ss"))
    
    # extract columns to create time table
    time_table = time_table.select("start_time",\
                           hour("start_time").alias("hour"),\
                           dayofmonth("start_time").alias("day"),\
                           weekofyear("start_time").alias("week"),\
                           month("start_time").alias("month"),\
                           year("start_time").alias("year"),\
                           date_format("start_time", 'E').alias("weekday"))
    

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month")\
    .parquet(output_data + "time",'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data" + "/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    
    songplays_table = song_df.join(df,(song_df.title == df.song) & \
                                      (song_df.artist_name == df.artist)) \
                    .select("songplay_id",\
                    to_timestamp("start_time","yyyy-MM-dd HH:mm:ss")\
                                 .alias("start_time"),\
                    col("userId").alias("user_id"),\
                    "level",\
                    "song_id",\
                    "artist_id",\
                    col("sessionId").alias("session_id"),\
                    "location",\
                    col("userAgent").alias("user_agent")
                    )
    
    songplays_table.show(n=5)

    # write songplays table to parquet files partitioned by year and month
    songplays_table\
    .withColumn("year", year("start_time"))\
    .withColumn("month",month("start_time"))\
    .write.partitionBy("year","month")\
    .parquet(output_data + "songplays", 'overwrite')
    

def main():
    """
    Main funtion that initiates the spark session and 
    defines location to read data from and write data to
    using spark
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"

    #Note: The output s3 bucket should be in same region as emr cluster
    output_data = "s3://nakuldefaultest/" 
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()

if __name__ == "__main__":
    main()
