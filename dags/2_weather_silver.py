import json
import io
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from weather_utils import build_daily_frame, payload_to_record, source_date_from_context


GCP_CONN_ID = "google_cloud_default"
BRONZE_BUCKET_VARIABLE = "weather_bronze_bucket"
SILVER_BUCKET_NAME = "de-weather-project-silver-gold"


def transform_bronze_to_silver(ds_nodash, dag_run=None, **_):
    """Build one complete, deduplicated Parquet file for a logical date."""
    conf = dag_run.conf if dag_run else None
    target_date = source_date_from_context(conf, ds_nodash)
    bronze_bucket_name = Variable.get(BRONZE_BUCKET_VARIABLE)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    all_keys = gcs_hook.list(bucket_name=bronze_bucket_name, prefix="bronze/") or []
    target_keys = sorted(key for key in all_keys if target_date in key)

    if not target_keys:
        raise AirflowException(f"No Bronze files found for logical date {target_date}")

    records = []
    failures = []

    for key in target_keys:
        try:
            content = gcs_hook.download(
                bucket_name=bronze_bucket_name,
                object_name=key,
            )
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

    gcs_hook.upload(
        bucket_name=SILVER_BUCKET_NAME,
        object_name=silver_key,
        data=parquet_buffer.getvalue(),
        mime_type="application/octet-stream",
    )
    print(
        f"Stored {len(frame)} unique records in "
        f"gs://{SILVER_BUCKET_NAME}/{silver_key}"
    )


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
