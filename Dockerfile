FROM apache/airflow:3.1.0-python3.11 as builder
ADD requirements.txt .
RUN uv pip install --no-cache apache-airflow==${AIRFLOW_VERSION} -r requirements.txt