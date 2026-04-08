# Use the official Airflow image as a base
FROM apache/airflow:2.10.0

# Set up arguments to fetch the correct constraints file
ARG AIRFLOW_VERSION=2.10.0
ARG PYTHON_VERSION_CONSTRAINTS=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION_CONSTRAINTS}.txt"

# Install packages using the constraints URL to prevent dependency backtracking
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    apache-airflow-providers-google \
    boto3 \
    pandas \
    pyarrow \
    s3fs \
    --constraint "${CONSTRAINT_URL}"