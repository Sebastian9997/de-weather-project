from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import os

def fetch_data_and_load_to_s3():
    # get API key from Airflow Variables
    api_key = Variable.get("owm_api_key")
    city = "Warsaw"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    response = requests.get(url)
    response.raise_for_status()  # Check for HTTP errors before proceeding
    data = response.json()

    # Connect to S3 and upload the file
    hook = S3Hook('aws_s3_conn')

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"weather_{timestamp}.json"

    print(f"Sending {filename} to S3...")
    hook.load_string(
        string_data=json.dumps(data),
        key=f"bronze/{filename}",
        bucket_name='de-weather-project-raw-data-owm',
        replace=True
    )
    print("Upload successful!")


# DAG definition
default_args = {
    'owner': 'sebastian',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '1_weather_collector_bronze',
    default_args=default_args,
    description='Raw data download from OpenWeatherMap and upload to S3',
    schedule_interval='@hourly', # Run every hour
    catchup=False,
    tags=['weather', 'bronze']
) as dag:
    task_get_data_and_load_to_s3 = PythonOperator(
        task_id='fetch_data_and_load_to_s3',
        python_callable=fetch_data_and_load_to_s3
    )