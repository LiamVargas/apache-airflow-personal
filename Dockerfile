FROM apache/airflow:3.1.0-python3.11

COPY my-sdk /opt/airflow/my-sdk

COPY requirements.txt /opt/airflow/requirements.txt

RUN uv pip install -e /opt/airflow/my-sdk

RUN uv pip install --no-cache -r /opt/airflow/requirements.txt