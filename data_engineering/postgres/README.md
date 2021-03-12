# Sparkify Song Play Analysis Database


## Summary of the Project

This project is part of the *Data Engineering Udacity nanodegree*.

It is used as a real-world scenario for **Data Modelling with Postgres**.


## Purpose of the Database

This database designed and implemented will be used to analyze the usage of the Sparkify, a music strearming App.

The purpose of the database is to help answer the question: *what songs users are listening to*.

This will be achieved by making data from the app usage, suitable and accessible for analytics.


## Database Schema and ETL pipeline

### Inputs

The input data for this database are derived from two different sources.

#### Song Dataset

This is a subset of the [Million Song Dataset](http://millionsongdataset.com/).
The dataset is split into JSON files, where each file contains metadata about a song and the artist of that song.

Example file (TRAABJL12903CDCF1A.json):

```json
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```

#### Log Dataset

A second datasource is the log files from the Application.

These are generated from the [event simulator](https://github.com/Interana/eventsim) and consist of JSON files with log entries.

```json
{
    "artist": null,
    "auth":" Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET",
    "page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId": "39"
}
```


### Design

The design of this database follows the Star schema. Various data are stored in dimension tables. This includes:
* artists
* songs
* time
* users

These are connected with the songplays table (the fact table), which stores events from when the users are requesting songs.

The tables together with their attributes are described below.


#### Artists and Songs tables

These are simple fact tables about artists and songs metadata with no external dependencies. Each row in the *songs* table references the artist of that song. 


#### Time table

This table contains information about each timestamp that we use in the database. For example, weekday, year, month etc.

It is worth noting that for the *time* table, we cannot use the start_time as a primary key and reference that from the *songplays* table because it is not unique.

The following query shows the rows where there is more than one time:
```sql
SELECT start_time, COUNT(start_time) FROM time GROUP BY start_time HAVING COUNT(start_time) > 1;
```

|start_time     |count|
|---------------|-----|
|18:46:02.796000|   2 |
|09:45:43.796000|   2 |
|17:42:35.796000|   2 |
|          ...        |


We use the unix timestamp as the primary key and ignore conflicts during insert.


#### Users table

Another interesting observation is that unlike song or artist metadata, users data are mutable.
This means that user properties can (and do) change through time.

We can see that the level one user changes between requests (free/paid):

|user_id | first_name | last_name | gender | level |
|--------|------------|-----------|--------|-------|
|15      |   Lily     |   Koch    |   F    | paid  |
|15      |   Lily     |   Koch    |   F    | free  |


We can find all of the users with changes in their profile using the following query:
```sql
SELECT user_id FROM users GROUP BY user_id HAVING COUNT(DISTINCT(first_name)) > 1 OR COUNT(DISTINCT(last_name)) > 1 OR COUNT(DISTINCT(level)) > 1 OR COUNT(DISTINCT(gender)) > 1;
```

##### Solution

One approach would be to only store the latest data for each user.
In other words, the *users* table would contain one row per user (identified by their userId) and store their current profile.


However, this contradicts purpose of this database.
We need to be able to understand usage patterns using historical data, thus it makes sense to store both the current and previous versions of each user's profile.


For simplicity, the *users* table, stores the data denormalized. This means that the user profile together with the timestamp is stored on each request.
The timestamp can be then used to join this table with the songplays table. 
If the dataset proves to be too large then this matrix will be normalized to include each user profile once.



## Files in this repository

| File             | Description                                                                                   |
|------------------|-----------------------------------------------------------------------------------------------|
| sql_queries.py   | Python script with static templates for creating/droping/inserting to tables for the database |
| create_tables.py | Python script to drop and create tables for the database                                      |
| etl.py           | Python script for generating a backfill of the Postgres database                              |
| test.ipynb       | Python notebook to verify that the backfill has run correctly                                 |
| etl.ipynb        | Python notebook with steps for developing the database                                        |


## How to run the Python Scripts

1. Running the ETL pipeline
Run the cells for creating the tables and running the ETL pipeline in *test.ipynb*

2. Running queries on the database
Run the select cells in the *test.ipynb* notebook