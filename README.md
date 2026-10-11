# Weather Data Pipeline on Google Cloud

## Project Goal

The goal of this project was to design and run a production-like, end-to-end data pipeline that continuously collects real weather data, processes it using a Medallion architecture, and exposes it for analytics and dashboards.

The project focuses on:
- reliable, idempotent orchestration with Airflow running 24/7
- raw and curated object storage in Google Cloud Storage
- cost-aware analytics architecture
- building a small but realistic analytical data platform

The pipeline has been running continuously for ~3 weeks and ingests hourly weather data for 8 European cities.

---

## Architecture Overview

### Pipeline flow:

    OpenWeather API
        ↓
    Shared Airflow platform (Docker on VPS)
        ↓
    Google Cloud Storage — Bronze (raw JSON)
        ↓
    Google Cloud Storage — Silver (daily Parquet)
        ↓
    BigQuery External Table
        ↓
    BigQuery Views (Gold)
        ↓
    Looker Studio Dashboard


---

### Architecture Evolution

The project originally stored Bronze and Silver data in AWS S3 and copied Silver
objects to GCS for BigQuery. This demonstrated a multi-cloud integration, but it
also introduced a second credentials set, IAM model, Airflow provider, and failure
point without a business requirement for two clouds.

The pipeline now keeps the same Bronze → Silver → Gold boundaries while using
GCS for both object-storage layers. Raw payloads are retained and can be
reprocessed from the GCS cutover date onward; only the redundant cross-cloud
transfer was removed.

The existing Silver object paths remain unchanged so the BigQuery External Table
and downstream dashboard do not need to move.

---

### Pipeline Details

#### Orchestration

The repository owns the weather DAGs and transformation logic. The Airflow runtime, Docker image, webserver, scheduler, and PostgreSQL metadata database are maintained separately in [portfolio-airflow-platform](https://github.com/Sebastian9997/portfolio-airflow-platform).

This separation allows one resource-conscious Airflow installation on the VPS to orchestrate multiple independent portfolio projects without making the weather repository responsible for unrelated pipelines.

The weather DAGs use:

- two retries with a five-minute delay;
- one active DAG run at a time;
- a dedicated single-slot `weather_pool`;
- dependency-based execution between Bronze and Silver;
- explicit failure when any city or source object is missing or invalid.

---

### Bronze Layer — Raw Data (Google Cloud Storage)

**Frequency:** hourly

**Volume:** 8 JSON files/hour (~0.1 MB/day)

Airflow calls the OpenWeatherMap API and stores raw responses in a dedicated GCS bucket.

Design decisions:

- one deterministic object key is generated per city and Airflow logical hour;
- retries overwrite the same logical-hour object instead of creating duplicates;
- all eight city responses are validated before the task can succeed;
- request timeouts and Airflow retries handle temporary API failures.

This makes retries and reprocessing idempotent while preserving hourly history.

---

### Silver Layer — Transformation (Google Cloud Storage)

The transformation DAG runs after ingestion completes.

Main tasks:

**JSON → Structured Dataset** 

The transformation step:
- reads all Bronze files for a given Airflow execution date
- validates that every expected city is represented
- rejects malformed source objects instead of silently producing partial data
- flattens nested JSON fields
- selects analytical columns
- converts UNIX timestamps to datetime
- enforces BigQuery-compatible datetime precision
- deduplicates records by city and extraction timestamp
- consolidates data from all cities

Output schema:

|column|description|
|---|---|
|city|city name|
temp|	temperature
humidity|	humidity
wind_speed|	wind speed
weather_desc|	weather description
lat / lon|	coordinates
extraction_time|	timestamp of measurement

Daily Parquet Strategy

Instead of writing hourly files, the pipeline:
- creates one Parquet file per day
- overwrites it hourly with appended data

Reason:
- avoids the small files problem
- improves query performance
- reduces metadata overhead

Output example:

**silver/daily_weather_YYYYMMDD.parquet**


---

### Direct GCS Storage

The Silver task reads Bronze JSON objects from GCS, transforms them in memory,
and uploads the current daily Parquet object directly to GCS. There is no
cross-cloud synchronization task.

This design requires one Google Cloud connection with permission to:
- list and read objects in the Bronze bucket;
- create and overwrite objects in the Bronze and Silver buckets.

Keeping Bronze and Silver in separate buckets allows their access and retention
policies to be managed independently.

---

### BigQuery — External Table

Instead of loading data into BigQuery storage, an External Table is defined over Parquet files in GCS using a wildcard path.

Key decisions:
- schema autodetection
- no data duplication inside BigQuery
- query data directly from Parquet

This significantly reduces storage costs while still enabling SQL analytics.

The dataset contains an extraction_date column used for filtering.

---

### Gold Layer — Analytical Views

The Gold layer is implemented as BigQuery views that filter the dataset to current-day data only.

Example logic:

    WHERE extraction_date = CURRENT_DATE()

This provides a clean and simple interface for BI tools.

---

### Dashboard (Looker Studio)

The dashboard provides:

Daily metrics per city
- min temperature
- max temperature
- average temperature
- average humidity
- average wind speed

Hourly trend chart
- temperature trends across the current day
- one line per city

Geographic visualization
- Europe map with bubbles representing cities
- bubble size & color = average daily temperature

https://lookerstudio.google.com/reporting/1e34ae0c-65fc-44e3-b483-be6c3d9feac6

---

### Key Engineering Decisions

#### Medallion Architecture

The pipeline follows Bronze → Silver → Gold layers to ensure:
- raw data preservation
- reproducible transformations
- clean analytical interface

#### Cost Optimization

Several decisions were driven by cost awareness:
- Parquet instead of JSON → cheaper queries
- Daily file aggregation → fewer small files
- BigQuery External Tables → no storage duplication
- Direct GCS writes → fewer credentials and pipeline failure points

---

### Challenges & Lessons Learned

Main challenges during development:

Running Airflow in Docker on a VPS
- container networking issues
- dependency/version conflicts
- multiple environment rebuilds

Cloud Authentication
- GCP service account configuration
- bucket-level IAM permissions

Architecture Redesign

Switching from the original AWS/GCP design to GCP-only storage required:
- starting a new raw-data history in GCS while retaining existing Silver history
- replacing the S3 Airflow hooks with the GCS hook
- removing the cross-cloud transfer step

Historical S3 raw objects were not migrated because the former AWS credentials were
no longer valid. Existing historical Silver files remain available in GCS, but raw
reprocessing is supported only for data collected after the cutover. This trade-off
avoids restoring an unused AWS account solely for a small portfolio dataset.

---

### Running and testing

Production orchestration is started from the [portfolio-airflow-platform](https://github.com/Sebastian9997/portfolio-airflow-platform) repository. That platform mounts this repository's `dags` directory read-only and supplies the required Airflow connections and variables:

- `google_cloud_default`
- `owm_api_key`
- `weather_bronze_bucket`

`weather_bronze_bucket` is an Airflow Variable containing the GCS bucket name
without the `gs://` prefix. The Silver bucket remains
`de-weather-project-silver-gold` so the existing BigQuery external-table path is stable.

Pure transformation and validation helpers can be tested without starting Airflow:

```bash
python -m unittest discover -s tests -v
```

The pipeline starts ingesting hourly after `1_weather_collector_bronze` is enabled in the shared Airflow UI.

---

### Tech Stack
- Apache Airflow
- Docker & docker-compose
- Google Cloud Storage
- BigQuery (External Tables + Views)
- Python (Pandas, PyArrow)
- Looker Studio

---

#### Author

#### Sebastian
