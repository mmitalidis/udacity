# DROP TABLES

sql_template_drop = """DROP TABLE IF EXISTS {table};"""

songplay_table_drop = sql_template_drop.format(table="songplay")
user_table_drop = sql_template_drop.format(table="users")
song_table_drop = sql_template_drop.format(table="song")
artist_table_drop = sql_template_drop.format(table="artist")
time_table_drop = sql_template_drop.format(table="time")

# CREATE TABLES


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGSERIAL PRIMARY KEY,
    timestamp INT,
    user_id INT,
    level VARCHAR(255),
    song_id VARCHAR(255),
    artist_id VARCHAR(255),
    session_id INT,
    location VARCHAR(255),
    user_agent VARCHAR(255),
    CONSTRAINT fk_time FOREIGN KEY (timestamp) REFERENCES time (timestamp),
    CONSTRAINT fk_users FOREIGN KEY (timestamp, user_id) REFERENCES users (timestamp, user_id) ON DELETE CASCADE,
    CONSTRAINT fk_songs FOREIGN KEY (song_id) REFERENCES songs (song_id) ON DELETE CASCADE,
    CONSTRAINT fk_artists FOREIGN KEY (artist_id) REFERENCES artists (artist_id) ON DELETE CASCADE
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    timestamp INT,
    user_id INT,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(255),
    level VARCHAR(255),
    PRIMARY KEY (timestamp, user_id),
    CONSTRAINT fk_time FOREIGN KEY (timestamp) REFERENCES time (timestamp)
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(255),
    year INT,
    duration REAL,
    CONSTRAINT fk_artists FOREIGN KEY (artist_id) REFERENCES artists (artist_id) ON DELETE CASCADE
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude REAL,
    longitude REAL
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    timestamp INT PRIMARY KEY,
    start_time time,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    timestamp,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users (timestamp, user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (timestamp, user_id) DO NOTHING;
""")


song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (
    timestamp,
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (timestamp) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id FROM songs as s JOIN artists as a ON s.artist_id = a.artist_id WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]