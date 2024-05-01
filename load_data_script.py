from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define DAG
dag = DAG(
    'load_csv_into_airflow',
    default_args=default_args,
    description='A DAG to load CSV file into Airflow',
    schedule_interval='@daily',
)

# Define task to load CSV file
def load_csv_task():
    file_path = '/home/henock/Downloads/your_csv_file.csv'
    df = pd.read_csv(file_path)
    # Process the DataFrame as needed
    # Example: df.to_sql('table_name', your_database_connection)

load_csv = PythonOperator(
    task_id='load_csv',
    python_callable=load_csv_task,
    dag=dag,
)

