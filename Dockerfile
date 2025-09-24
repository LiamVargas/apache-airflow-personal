FROM apache/airflow:3.0.6-python3.11 as builder
ADD requirements.txt .
RUN uv pip install --no-cache apache-airflow==${AIRFLOW_VERSION} -r requirements.txt