import os
import re
import glob
from io import StringIO

import psycopg2
from psycopg2.extensions import register_adapter, AsIs

import numpy as np
import pandas as pd
from sql_queries import *

register_adapter(np.int64, AsIs)

def make_songplays_df(cur, df, time_df):
    """
    Create the pandas DataFrame to be stored in the songplays table.
    """
    
    # Extract the song, artist, duration from the logs and store them in a temporary table
    sql = """
    CREATE TEMP TABLE tmp_song_data_query (
        title VARCHAR(255),
        name VARCHAR(255),
        duration REAL
    )
    ON COMMIT DROP;

    COPY tmp_song_data_query FROM STDIN WITH CSV;
    """
    song_data = df[["song", "artist", "length"]].rename(
        columns={"song": "title", "artist": "name", "length": "duration"}
    )
    cur.copy_expert(sql, StringIO(song_data.to_csv(sep=",", na_rep="", index=False, header=False)))
    
    # Join the tables songs, artists and the temporary table with the data that we want to query 
    # and extract the song_id and artist_id
    cur.execute("""
    SELECT s.song_id, a.artist_id FROM
    
    songs as s JOIN artists as a 
    ON s.artist_id = a.artist_id
    
    RIGHT JOIN tmp_song_data_query as t
    ON s.title = t.title AND a.name = t.name;
    """)
    song_artist_ids = pd.DataFrame(cur.fetchall(), columns=["song_id", "artist_id"])
    
    songplays_df = pd.concat([
        time_df.timestamp, df[["userId", "level"]], song_artist_ids, df[["sessionId", "location", "userAgent"]]
    ], axis=1).reset_index()
    songplays_df.columns = ["songplay_id", "timestamp", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent"]
    return songplays_df


def copy_df_to_db(cur, df, table):
    """
    Copy a pandas DataFrame to the Database using the COPY operation.
    """    
    df_str = StringIO(df.to_csv(sep=",", na_rep="", index=False, header=False))
    table = re.escape(table)
    sql = f"""
    CREATE TEMP TABLE tmp_{table} 
    (LIKE {table} INCLUDING DEFAULTS)
    ON COMMIT DROP;

    COPY tmp_{table} FROM STDIN WITH CSV;

    INSERT INTO {table}
    SELECT *
    FROM tmp_{table}
    ON CONFLICT DO NOTHING;
    """
    cur.copy_expert(sql, df_str)


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    copy_df_to_db(cur, artist_data, "artists")

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    copy_df_to_db(cur, song_data, "songs")


def process_log_file(cur, filepath):
    # open log file
    df = df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"].reset_index(drop=True)

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit="ms")

    # insert time data records
    def to_unix(series):
        return (series - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")

    time_data = (to_unix(t), t.dt.time, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("timestamp", "start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.concat(time_data, axis=1)
    time_df.columns = column_labels

    copy_df_to_db(cur, time_df, "time")

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].rename(
        columns={"userId": "user_id", "firstName": "first_name", "lastName": "last_name"})
    user_df = pd.concat([time_df.timestamp, user_df], axis=1)

    # insert user records
    copy_df_to_db(cur, user_df, "users")

    # insert songplays records
    songplays_df = make_songplays_df(cur, df, time_df)
    copy_df_to_db(cur, songplays_df, "songplays")
    

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    lines = []
    for datafile in all_files:
        with open(datafile, "r") as fin:
            lines.extend(fin.readlines())
    text = StringIO('\n'.join(lines))
    print(f'Read {num_files} files.')
    
    print("Processing...", end="")
    func(cur, text)
    conn.commit()
    print("DONE")
        


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()