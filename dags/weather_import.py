from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import os

# Function to fetch weather data from OpenWeatherMap API
def fetch_weather():
    # get API key from Airflow Variables
    api_key = Variable.get("owm_api_key")
    city = "Warsaw"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    response = requests.get(url)
    data = response.json()
    
    # Save data to local file in bronze layer
    output_dir = "/opt/airflow/data/bronze"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"weather_{timestamp}.json"
    
    with open(f"{output_dir}/{filename}", "w") as f:
        json.dump(data, f)
    
    print(f"Data saved successfully to: {filename}")


def upload_to_s3(ti):
    # Download the latest file from bronze and upload to S3
    # for simplicity, we will just take the latest file in the bronze directory
    import os
    source_dir = "/opt/airflow/data/bronze"
    files = [f for f in os.listdir(source_dir) if f.endswith('.json')]
    if not files:
        print("No files found in bronze directory to upload.")
        return
    
    last_file = sorted(files)[-1]
    file_path = os.path.join(source_dir, last_file)
    
    # Connect to S3 and upload the file
    hook = S3Hook('aws_s3_conn') 
    
    print(f"Sending {last_file} to S3...")
    hook.load_file(
        filename=file_path,
        key=f"bronze/{last_file}",
        bucket_name='de-weather-project-raw-data-owm',
        replace=True
    )
    print("Upload successful!")

# DAG definition
default_args = {
    'owner': 'sebastian',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '1_weather_collector_bronze',
    default_args=default_args,
    description='Raw data download from OpenWeatherMap',
    schedule_interval='@hourly', # Run every hour
    catchup=False,
    tags=['weather', 'bronze']
) as dag:

    task_get_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather
    )

    upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3
)
    
    task_get_weather >> upload_task