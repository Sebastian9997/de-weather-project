import json
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from weather_utils import EXPECTED_CITIES, bronze_object_key, payload_to_record


GCP_CONN_ID = "google_cloud_default"
BRONZE_BUCKET_VARIABLE = "weather_bronze_bucket"

CITY_COORDINATES = {
    "Warsaw": (52.2297, 21.0122),
    "Berlin": (52.5200, 13.4050),
    "Paris": (48.8566, 2.3522),
    "London": (51.5072, -0.1276),
    "Madrid": (40.4168, -3.7038),
    "Rome": (41.9028, 12.4964),
    "Prague": (50.0755, 14.4378),
    "Vienna": (48.2082, 16.3738),
}


def fetch_data_and_load_to_gcs(data_interval_end, **_):
    """Fetch a complete city snapshot and store it under retry-safe GCS keys."""
    api_key = Variable.get("owm_api_key")
    bronze_bucket_name = Variable.get(BRONZE_BUCKET_VARIABLE)
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    failures = []

    for city_name in EXPECTED_CITIES:
        lat, lon = CITY_COORDINATES[city_name]
        url = "https://api.openweathermap.org/data/3.0/onecall"
        params = {
            "lat": lat,
            "lon": lon,
            "exclude": "minutely,hourly,daily,alerts",
            "appid": api_key,
            "units": "metric",
        }

        try:
            response = requests.get(url, params=params, timeout=(5, 30))
            response.raise_for_status()
            data = response.json()
            data["city"] = city_name
            payload_to_record(data)
            bronze_key = bronze_object_key(city_name, data_interval_end)
            hook.upload(
                bucket_name=bronze_bucket_name,
                object_name=bronze_key,
                data=json.dumps(data),
                mime_type="application/json",
            )
            print(
                f"Stored a validated payload for {city_name} at "
                f"gs://{bronze_bucket_name}/{bronze_key}"
            )
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            failures.append(f"{city_name}: HTTP status {status}")
        except requests.RequestException as exc:
            failures.append(f"{city_name}: {type(exc).__name__}")
        except Exception as exc:
            failures.append(f"{city_name}: {type(exc).__name__}: {exc}")

    if failures:
        raise AirflowException("Incomplete weather snapshot: " + " | ".join(failures))


default_args = {
    "owner": "sebastian",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 3, 12, tz="UTC"),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "1_weather_collector_bronze",
    default_args=default_args,
    description="Download a complete OpenWeather snapshot to GCS Bronze",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "bronze"],
) as dag:
    task_get_data_and_load_to_gcs = PythonOperator(
        task_id="fetch_data_and_load_to_gcs",
        python_callable=fetch_data_and_load_to_gcs,
        pool="weather_pool",
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_layer",
        trigger_dag_id="2_weather_silver_transformation",
        conf={
            "source_ds_nodash": "{{ data_interval_end.in_timezone('UTC').strftime('%Y%m%d') }}"
        },
        wait_for_completion=False,
        pool="weather_pool",
    )

    task_get_data_and_load_to_gcs >> trigger_silver
