# Multi-Cloud Weather Data Pipeline

## Project Goal

The goal of this project was to design and run a production-like, end-to-end data pipeline that continuously collects real weather data, processes it using a Medallion architecture, and exposes it for analytics and dashboards.

The project focuses on:
- orchestration with Airflow running 24/7
- multi-cloud data movement (AWS → GCP)
- cost-aware analytics architecture
- building a small but realistic analytical data platform

The pipeline has been running continuously for ~3 weeks and ingests hourly weather data for 8 European cities.

---

## Architecture Overview

### Pipeline flow:

    OpenWeather API
        ↓
    Airflow (Docker on VPS)
        ↓
    AWS S3 — Bronze (raw JSON)
        ↓
    AWS S3 — Silver (daily Parquet)
        ↓
    Transfer: S3 → Google Cloud Storage
        ↓
    BigQuery External Table
        ↓
    BigQuery Views (Gold)
        ↓
    Looker Studio Dashboard


---

### Why Multi-Cloud?

Originally the plan was to query data directly from AWS using Athena and connect it to Looker Studio.
However, the native Athena connector is paid, which made the architecture impractical for a personal project.

Instead, the pipeline was redesigned to:
- keep ingestion and storage in AWS
- move curated data to GCP for analytics
- use BigQuery External Tables to avoid data duplication costs

This became a deliberate cost-optimization and multi-cloud integration exercise.

---

### Pipeline Details

#### Orchestration

Airflow runs 24/7 on a VPS inside Docker using docker-compose.

Services:
- airflow-webserver
- airflow-scheduler
- postgres (metadata DB)

Each DAG has:
- retry policy: 1 retry after 5 minutes
- dependency-based execution between layers

One of the initial challenges was getting Airflow running reliably in containers and resolving dependency/version issues.

---

### Bronze Layer — Raw Data (AWS S3)

**Frequency:** hourly

**Volume:** 8 JSON files/hour (~0.1 MB/day)

Airflow calls the OpenWeatherMap API and stores raw responses in S3.

Design decision:
- Raw files are stored immutably
- Files are filtered by Airflow logical date (ds_nodash) to make the DAG time-travel safe

This allows safe backfills and reprocessing.

---

### Silver Layer — Transformation (AWS S3)

The transformation DAG runs after ingestion completes.

Main tasks:

**JSON → Structured Dataset** 

The transformation step:
- reads all Bronze files for a given Airflow execution date
- flattens nested JSON fields
- selects analytical columns
- converts UNIX timestamps to datetime
- enforces BigQuery-compatible datetime precision
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

### Data Transfer: AWS → GCP

Airflow uses S3ToGCSOperator to synchronize the Silver layer to Google Cloud Storage.

This step required:
- configuring credentials for two cloud providers
- resolving IAM permissions
- enabling GCS service account key creation (disabled by default)

Multi-cloud authentication and permissions setup was one of the most time-consuming parts of the project.

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
- Multi-cloud workaround → avoided paid Athena connector

---

### Challenges & Lessons Learned

Main challenges during development:

Running Airflow in Docker on a VPS
- container networking issues
- dependency/version conflicts
- multiple environment rebuilds

Multi-Cloud Authentication
- AWS credential management
- GCP service account configuration
- IAM permissions and default restrictions

Architecture Redesign

Switching from Athena to BigQuery required:
- introducing GCS
- building a cross-cloud transfer step
- redesigning the analytics layer

This significantly improved understanding of real-world cloud integration challenges.

---

#### How to Run the Project
1. Configure environment variables:
    - AWS credentials
	- GCP service account key
	- OpenWeather API key
2. Start Airflow:
    
        docker-compose up

3. Access Airflow UI and enable DAGs.

The pipeline will start ingesting data hourly.

---

### Tech Stack
- Apache Airflow
- Docker & docker-compose
- AWS S3
- Google Cloud Storage
- BigQuery (External Tables + Views)
- Python (Pandas, PyArrow)
- Looker Studio

---

#### Author

#### Sebastian