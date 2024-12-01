from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from Scripts.API_Call import API_Call
from airflow.utils.dates import days_ago

# Set default arguments
default_args = {
    'owner': 'airflow',
    'start_date': None,  # Set to today's date
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define your DAG
with DAG(
    'WeatherAPI',
    default_args=default_args,
    schedule_interval='@once',  # Trigger once
    max_active_runs=1,
    catchup=False
) as dag:

    # Use PythonOperator to call the imported function
    run_API_Call = PythonOperator(
        task_id='API_Call',
        python_callable=API_Call,  # Reference the imported function
        dag=dag
    )

run_API_Call
