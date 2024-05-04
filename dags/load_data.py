from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG
import os
from airflow.hooks.postgres_hook import PostgresHook




csv_file_path = "/home/henock/Desktop/10_Academy/week_2/Airflow/dags/new_traffic.csv"

default_args ={
    'owner': 'Henock',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='load_data',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
) as dag:

    create_staging_table_task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS traffic_table (
                track_id INTEGER,
                type VARCHAR,
                traveled_d FLOAT,
                avg_speed FLOAT,
                lat FLOAT,
                lon FLOAT,
                speed FLOAT,
                lon_acc FLOAT,
                lat_acc FLOAT,
                time FLOAT
            );
        """
        )
    
     # Task to load data from CSV to PostgreSQL
    load_csv_to_postgres_task = PostgresOperator(
        task_id='load_csv_to_postgres',
        postgres_conn_id='postgres_localhost',  # Connection ID configured in Airflow for PostgreSQL
        sql=f"""
            COPY traffic_table
            FROM '{csv_file_path}'
            WITH CSV HEADER;
        """
    )
    
  
    create_staging_table_task >> load_csv_to_postgres_task 
