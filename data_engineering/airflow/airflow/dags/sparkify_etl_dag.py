from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


# In order to run this pipeline with the data currently stored in the S3 buckets:
# -------------------
# 1. Create the tables by running the create_tables.sql file.
#
# 2. Add the Redsfhit Connection Id in Airflow/Admin/Connections.
#
# 3. Replace `S3toRedshiftRole` with the ARN AWS IAM role for accessing S3 from Redshift,
#    that is already associated with the Redshift cluster.
#
# 4. Set the schedule_interval to "@monthly".
#
# 5. Set catchup=True (events data are only available in the for Nov-2018).
#
# 6. Set: "end_date": datetime(2018, 11, 29) in the DAG default args.
S3toRedshiftRole = "arn:aws:iam::643756010537:role/myRedshiftRole"


with DAG(
    "sparkify_etl",
    default_args={
        "owner": "udacity",
        "start_date": datetime(2018, 11, 1),
        "depends_on_past": False,
        "email_on_retry": False,
        "retry_delay": timedelta(seconds=300),
        "retries": 3,
    },
    description="Load and transform data in Redshift with Airflow",
    max_active_runs=1,
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        s3_object="s3://udacity-dend/log_data/{year}/{month}",
        json_path="s3://udacity-dend/log_json_path.json",
        template_s3_object=True,
        iam_role=S3toRedshiftRole,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        s3_object="s3://udacity-dend/song_data",
        template_s3_object=False,
        iam_role=S3toRedshiftRole,
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        select_query=SqlQueries.songplay_table_insert,
        select_from_multiple_tables=["staging_events", "staging_songs"],
        insert_into_table="songplays",
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        select_query=SqlQueries.user_table_insert,
        select_from_table="staging_events",
        insert_into_table="users",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        select_query=SqlQueries.song_table_insert,
        select_from_table="staging_songs",
        insert_into_table="songs",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        select_query=SqlQueries.artist_table_insert,
        select_from_table="staging_songs",
        insert_into_table="artists",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        select_query=SqlQueries.time_table_insert,
        select_from_table="songplays",
        insert_into_table="time",
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        sql_queries=["select count(first_name) from users where first_name is NULL"],
        expected_results=[(0,)],
    )

    end_operator = DummyOperator(task_id="Stop_execution")


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
