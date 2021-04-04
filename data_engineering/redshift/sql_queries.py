import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

drop_sql = "DROP TABLE IF EXISTS {};"

staging_events_table_drop = drop_sql.format("staging_events")
staging_songs_table_drop = drop_sql.format("staging_songs")
songplay_table_drop = drop_sql.format("songplays")
user_table_drop = drop_sql.format("users")
song_table_drop = drop_sql.format("songs")
artist_table_drop = drop_sql.format("artists")
time_table_drop = drop_sql.format("time_tables")

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT NOT NULL,
    lastName VARCHAR,
    length REAL,
    level VARCHAR NOT NULL,
    location VARCHAR,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration REAL,
    sessionId INT NOT NULL,
    song VARCHAR,
    status SMALLINT NOT NULL,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    userId BIGINT
    );
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR,
    artist_latitude REAL,
    artist_location VARCHAR,
    artist_longitude REAL,
    artist_name VARCHAR NOT NULL,
    duration REAL NOT NULL,
    num_songs INT NOT NULL,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    year INT NOT NULL
    );
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT NOT NULL IDENTITY(1, 1),
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id VARCHAR DISTKEY,
    level VARCHAR  NOT NULL,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id VARCHAR NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
    );
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT DISTKEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
    );
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL SORTKEY,
    artist_id VARCHAR NOT NULL,
    year VARCHAR NOT NULL,
    duration REAL NOT NULL
    )
    DISTSTYLE ALL;
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR,
    name VARCHAR NOT NULL SORTKEY,
    location VARCHAR,
    latitude REAL,
    longitude REAL
    )
    DISTSTYLE ALL;
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS times (
    start_time TIMESTAMP SORTKEY,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year INT,
    weekday SMALLINT
    );
"""

# STAGING TABLES

staging_events_copy = (
    """
COPY staging_events
FROM '{s3_object}'
IAM_ROLE '{iam_role}'
JSON '{json_path}'
REGION '{region}';
"""
).format(
    s3_object=config["S3"]["LOG_DATA"],
    iam_role=config["IAM_ROLE"]["ARN"],
    json_path=config["S3"]["LOG_JSONPATH"],
    region=config["S3"]["REGION"],
)

staging_songs_copy = (
    """
COPY staging_songs
FROM '{s3_object}'
IAM_ROLE '{iam_role}'
JSON 'auto'
REGION '{region}';
"""
).format(
    s3_object=config["S3"]["SONG_DATA"],
    iam_role=config["IAM_ROLE"]["ARN"],
    region=config["S3"]["REGION"],
)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    DATEADD('milliseconds', e.ts, '1970-01-01') as start_time,
    e.userId,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    s.artist_location,
    e.userAgent
FROM staging_events e JOIN staging_songs s
ON e.song = s.title
WHERE e.page = 'NextSong';
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events;
"""


song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
"""


artist_table_insert = """
INSERT INTO artists (artist_id, name, location, longitude, latitude)
SELECT DISTINCT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_longitude AS longitude,
    artist_latitude AS latitude
FROM staging_songs;
"""


time_table_insert = """
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time,
    EXTRACT (hour from start_time),
    EXTRACT (day from start_time),
    EXTRACT (week from start_time),
    EXTRACT (month from start_time),
    EXTRACT (year from start_time),
    EXTRACT (weekday from start_time)
FROM songplays;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
load_staging_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
