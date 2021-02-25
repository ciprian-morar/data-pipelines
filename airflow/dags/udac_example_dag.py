from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from udac_example_subdag import get_s3_to_redshift_dag
from helpers import SqlQueries


default_args = {
    'owner': 'ciprian',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup':False,
    'retries':0,
    'email_on_retry':False,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once'      
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_task_id = 'stage_events_subdag'
stage_events_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "udac_example_dag",
        stage_events_task_id,
        "redshift",
        "aws_credentials",
        "staging_events",
        create_sql_stmt=SqlQueries.staging_events_table_create,
        s3_bucket="udacity-dend",
        s3_key="log_data",
        region='us-west-2',
        json='s3://udacity-dend/log_json_path.json',
        start_date=default_args.get("start_date"),
    ),
    task_id=stage_events_task_id,
    dag=dag,
)

stage_songs_task_id = 'stage_songs_subdag'
stage_songs_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "udac_example_dag",
        stage_songs_task_id,
        "redshift",
        "aws_credentials",
        "staging_songs",
        create_sql_stmt=SqlQueries.staging_songs_table_create,
        s3_bucket="udacity-dend",
        s3_key="song_data",
        region='us-west-2',
        json='auto',
        start_date=default_args.get("start_date"),
    ),
    task_id=stage_songs_task_id,
    dag=dag,
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    create_table=SqlQueries.songplay_table_create,
    insert_table=SqlQueries.songplay_table_insert,
    delete_flag=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    create_table=SqlQueries.user_table_create,
    insert_table=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    create_table=SqlQueries.song_table_create,
    insert_table=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    create_table=SqlQueries.artist_table_create,
    insert_table=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    create_table=SqlQueries.time_table_create,
    insert_table=SqlQueries.time_table_insert,
)


query_checks=[
    	{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM songplays WHERE userid is null", 'expected_result':0}    	
    ]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    query_checks=query_checks,
    table_names=('songplays', 'songs', 'users', 'artists', 'time')
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
#temp
# start_operator >> load_songplays_table
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
