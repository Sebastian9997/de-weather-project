from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import json
import os

def fetch_data_and_load_to_s3():
    # get API key from Airflow Variables
    api_key = Variable.get("owm_api_key")

    # Connect to S3 using Airflow's S3Hook
    hook = S3Hook('aws_s3_conn')

    capitals = [
        {"name": "Warsaw", "lat": 52.2297, "lon": 21.0122},
        {"name": "Berlin", "lat": 52.5200, "lon": 13.4050},
        {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
        {"name": "London", "lat": 51.5072, "lon": -0.1276},
        {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
        {"name": "Rome", "lat": 41.9028, "lon": 12.4964},
        {"name": "Prague", "lat": 50.0755, "lon": 14.4378},
        {"name": "Vienna", "lat": 48.2082, "lon": 16.3738}
    ]

    for city in capitals:
        city_name = city["name"]
        lat = city["lat"]
        lon = city["lon"]

        # New One Call API 3.0 URL
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=minutely,hourly,daily,alerts&appid={api_key}&units=metric"

        try:
            response = requests.get(url)
            response.raise_for_status() 
            data = response.json()
            data['city'] = city_name  # Add city name to the data for better identification

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{city_name.lower()}_{timestamp}.json"
            s3_key = f"bronze/{city_name.lower()}/{filename}"

            print(f"Sending {city_name} data to S3: {s3_key}")
            
            hook.load_string(
                string_data=json.dumps(data),
                key=s3_key,
                bucket_name='de-weather-project-raw-data-owm',
                replace=True
            )
        except Exception as e:
            print(f"Error fetching data for {city_name}: {e}")
            continue

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
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather', 'bronze']
) as dag:
    task_get_data_and_load_to_s3 = PythonOperator(
        task_id='fetch_data_and_load_to_s3',
        python_callable=fetch_data_and_load_to_s3
    )

    trigger_silver = TriggerDagRunOperator(
    task_id="trigger_silver_layer",
    trigger_dag_id="2_weather_silver_transformation",
)

task_get_data_and_load_to_s3 >> trigger_silver