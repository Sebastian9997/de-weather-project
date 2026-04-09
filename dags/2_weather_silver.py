import json
import io
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

# configuration for S3 and bucket
S3_CONN_ID = 'aws_s3_conn'
BUCKET_NAME = 'de-weather-project-raw-data-owm'

def transform_bronze_to_silver(ds_nodash, **kwargs):
    """
    ds_nodash: Airflow automatically passes the date in YYYYMMDD format.
    Example: '20260318'
    """
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    
    # look for all files from the SPECIFIC logical date (ds_nodash)
   
    all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix='bronze/')
    
    # filter keys that match the date Airflow is currently processing
    target_keys = [k for k in all_keys if ds_nodash in k]
    
    if not target_keys:
        print(f"No Bronze files found for logical date {ds_nodash}. Skipping.")
        return

    all_data = []

    # Extract and Flatten
    for key in target_keys:
        try:
            content = s3_hook.read_key(key, bucket_name=BUCKET_NAME)
            data = json.loads(content)
            
            all_data.append({
                'city': data.get('city'),
                'temp': data['current']['temp'],
                'humidity': data['current']['humidity'],
                'wind_speed': data['current']['wind_speed'],
                'weather_desc': data['current']['weather'][0]['description'],
                'lat': data.get('lat'),
                'lon': data.get('lon'),
                # Convert OWM unix timestamp to readable datetime
                'extraction_time': pd.to_datetime(data['current']['dt'], unit='s')
            })
        except Exception as e:
            print(f"Error processing key {key}: {e}")

    df = pd.DataFrame(all_data)

    # Force the datetime column down to microsecond precision for BigQuery compatibility
    df['extraction_time'] = df['extraction_time'].astype('datetime64[us]')
    
    # Convert to Parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    
    # Save as a single daily file (Overwrites every hour with latest data)
    silver_key = f"silver/daily_weather_{ds_nodash}.parquet"
    
    s3_hook.load_bytes(
        parquet_buffer.getvalue(), 
        key=silver_key, 
        bucket_name=BUCKET_NAME, 
        replace=True
    )
    
    print(f"Successfully processed {len(df)} records into {silver_key}")

# DAG DEFINITION
default_args = {
    'owner': 'sebastian',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '2_weather_silver_transformation',
    default_args=default_args,
    description='Aggregates hourly Bronze JSONs into a daily Silver Parquet file',
    schedule_interval=None, # Runs only when triggered by the Bronze DAG"
    catchup=False
) as dag:

    transform_task = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=transform_bronze_to_silver
    )

    sync_to_gcp = S3ToGCSOperator(
    task_id='sync_s3_to_gcs',
    bucket='de-weather-project-raw-data-owm',
    prefix='silver/daily_weather_',
    dest_gcs='gs://de-weather-project-silver-gold/',
    aws_conn_id='aws_s3_conn',
    gcp_conn_id='google_cloud_default',
    replace=True
)

    transform_task >> sync_to_gcp