import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


def create_spark_session() -> SparkSession:
    """Initializes and SparkSession"""
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def read_song_data(spark: SparkSession, song_data_path: str) -> DataFrame:
    """Reads the song_data DataFrame."""
    song_data_schema = StructType(
        [
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_name", StringType()),
            StructField("duration", DoubleType()),
            StructField("num_songs", LongType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("year", LongType()),
        ]
    )
    song_data = spark.read.json(song_data_path, schema=song_data_schema)
    return song_data


def read_log_data(spark: SparkSession, log_data_path: str) -> DataFrame:
    """Reads the log_data DataFrame."""
    log_data_schema = StructType(
        [
            StructField("artist", StringType()),
            StructField("auth", StringType()),
            StructField("firstName", StringType()),
            StructField("gender", StringType()),
            StructField("itemInSession", LongType()),
            StructField("lastName", StringType()),
            StructField("length", DoubleType()),
            StructField("level", StringType()),
            StructField("location", StringType()),
            StructField("method", StringType()),
            StructField("page", StringType()),
            StructField("registration", DoubleType()),
            StructField("sessionId", LongType()),
            StructField("song", StringType()),
            StructField("status", LongType()),
            StructField("ts", LongType()),
            StructField("userAgent", StringType()),
            StructField("userId", StringType()),
        ]
    )
    log_data = spark.read.json(log_data_path, schema=log_data_schema)
    return log_data


def clean_log_data(df: DataFrame) -> DataFrame:
    """
    Preprocesses the log_data DataFrame.

    This method transforms the log_data DataFrame to make the creation of tables easier.
    - It renames the columns to snake_notation.
    - Filters the rows by selecting only those which are relevant for the song play analysis.
    - Adds a new column (start_time), of type timestamp.
    """
    df = (
        df.withColumnRenamed("userId", "user_id")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("userAgent", "user_agent")
    )
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")
    df = df.withColumn(
        "start_time", f.from_unixtime(f.col("ts") / 1000).cast(TimestampType())
    )
    return df


def filter_song_data(song_data: DataFrame, log_data: DataFrame) -> DataFrame:
    """Keeps only the rows of the song_data table which have appeared in the app logs"""
    song_data = song_data.join(
        log_data,
        on=[
            song_data.title == log_data.song,
            song_data.duration == log_data.length,
            song_data.artist_name == log_data.artist,
        ],
        how="left_semi",
    )
    return song_data


def make_songs_table(song_data: DataFrame) -> DataFrame:
    """Creates the songs_table."""
    songs_table = song_data.select("song_id", "title", "artist_id", "year", "duration")
    return songs_table


def make_artists_table(song_data: DataFrame) -> DataFrame:
    """Creates the artists_table."""
    artists_table = (
        song_data.selectExpr(
            "artist_id",
            "artist_name as name",
            "artist_location as location",
            "artist_latitude as latitude",
            "artist_longitude as longtitude",
        )
        .filter("artist_id is not null")
        .filter("name is not null")
        .dropDuplicates(["artist_id"])
    )
    return artists_table


def make_users_table(log_data: DataFrame) -> DataFrame:
    """Creates the users_table."""
    user_id = Window.partitionBy("user_id")
    users_table = (
        log_data.select("user_id", "first_name", "last_name", "gender", "level", "ts")
        .withColumn("maxTs", f.max("ts").over(user_id))
        .where(f.col("ts") == f.col("maxTs"))
        .dropDuplicates(["ts"])
        .drop("ts", "maxTs")
    )
    return users_table


def make_times_table(log_data: DataFrame) -> DataFrame:
    """Creates the times_table."""
    timestamp = log_data.select("start_time").dropDuplicates()

    times_table = timestamp.select(
        "start_time",
        f.hour("start_time").alias("hour"),
        f.dayofyear("start_time").alias("day"),
        f.weekofyear("start_time").alias("week"),
        f.month("start_time").alias("month"),
        f.year("start_time").alias("year"),
        f.dayofweek("start_time").alias("weekday"),
    )
    return times_table


def make_songplays_table(
    log_data: DataFrame, songs_table: DataFrame, artists_table: DataFrame
) -> DataFrame:
    """Creates the songplays_table."""
    song_df = songs_table.join(artists_table, on="artist_id").drop("location")
    song_df = song_df.join(
        log_data,
        on=[
            song_df.title == log_data.song,
            song_df.duration == log_data.length,
            song_df.name == log_data.artist,
        ],
    )
    song_df = song_df.withColumn("songplay_id", f.monotonically_increasing_id())

    songplays_table = song_df.select(
        "songplay_id",
        "start_time",
        "user_id",
        "level",
        "song_id",
        "artist_id",
        "session_id",
        "location",
        "user_agent",
        f.month("start_time").alias("month"),
        f.year("start_time").alias("year"),
    )
    return songplays_table


def main() -> None:
    """ETL process to read data raw data, transform to a star schema and write them back."""
    spark: SparkSession = create_spark_session()

    log_data: DataFrame = read_log_data(
        spark, log_data_path="s3a://udacity-dend/log_data/*/*/*-events.json"
    )
    log_data: DataFrame = clean_log_data(log_data)

    song_data: DataFrame = read_song_data(
        spark, song_data_path="s3a://udacity-dend/song_data/*/*/*/*.json"
    )
    song_data: DataFrame = filter_song_data(song_data, log_data)

    songs_table: DataFrame = make_songs_table(song_data)
    artists_table: DataFrame = make_artists_table(song_data)
    users_table: DataFrame = make_users_table(log_data)
    times_table: DataFrame = make_times_table(log_data)
    songplays_table: DataFrame = make_songplays_table(
        log_data, songs_table, artists_table
    )

    output_path = "s3a://udacity-mm-spark-etl-test"
    songs_table.write.parquet(
        os.path.join(output_path, "songs_table.parquet"),
        mode="overwrite",
        partitionBy=["year", "artist_id"],
    )
    artists_table.write.parquet(
        os.path.join(output_path, "artists_table.parquet"), mode="overwrite"
    )
    users_table.write.parquet(
        os.path.join(output_path, "users_table.parquet"), mode="overwrite"
    )
    times_table.write.parquet(
        os.path.join(output_path, "times_table.parquet"),
        mode="overwrite",
        partitionBy=["year", "month"],
    )
    songplays_table.write.parquet(
        os.path.join(output_path, "songplays_table.parquet"),
        mode="overwrite",
        partitionBy=["year", "month"],
    )


if __name__ == "__main__":
    main()
