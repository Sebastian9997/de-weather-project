import json
import io
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

from weather_utils import build_daily_frame, payload_to_record, source_date_from_context


S3_CONN_ID = "aws_s3_conn"
BUCKET_NAME = "de-weather-project-raw-data-owm"


def transform_bronze_to_silver(ds_nodash, dag_run=None, **_):
    """Build one complete, deduplicated Parquet file for a logical date."""
    conf = dag_run.conf if dag_run else None
    target_date = source_date_from_context(conf, ds_nodash)
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix="bronze/") or []
    target_keys = sorted(key for key in all_keys if target_date in key)

    if not target_keys:
        raise AirflowException(f"No Bronze files found for logical date {target_date}")

    records = []
    failures = []

    for key in target_keys:
        try:
            content = s3_hook.read_key(key, bucket_name=BUCKET_NAME)
            data = json.loads(content)
            records.append(payload_to_record(data))
        except Exception as exc:
            failures.append(f"{key}: {type(exc).__name__}: {exc}")

    if failures:
        raise AirflowException("Invalid Bronze objects: " + " | ".join(failures))

    frame = build_daily_frame(records)
    parquet_buffer = io.BytesIO()
    frame.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    silver_key = f"silver/daily_weather_{target_date}.parquet"

    s3_hook.load_bytes(
        parquet_buffer.getvalue(),
        key=silver_key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )
    print(f"Stored {len(frame)} unique records in {silver_key}")


default_args = {
    "owner": "sebastian",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 3, 18, tz="UTC"),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "2_weather_silver_transformation",
    default_args=default_args,
    description="Aggregate validated Bronze objects into daily Silver Parquet",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["weather", "silver"],
) as dag:
    transform_task = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_bronze_to_silver,
        pool="weather_pool",
    )

    sync_to_gcp = S3ToGCSOperator(
        task_id="sync_s3_to_gcs",
        bucket=BUCKET_NAME,
        prefix=(
            "silver/daily_weather_"
            "{{ dag_run.conf.get('source_ds_nodash', ds_nodash) "
            "if dag_run and dag_run.conf else ds_nodash }}.parquet"
        ),
        dest_gcs="gs://de-weather-project-silver-gold/",
        aws_conn_id=S3_CONN_ID,
        gcp_conn_id="google_cloud_default",
        replace=True,
        pool="weather_pool",
    )

    transform_task >> sync_to_gcp
