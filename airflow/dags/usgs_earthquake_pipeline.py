import os
import logging
from datetime import datetime, timedelta
import requests
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateExternalTableOperator,
)
from google.cloud import storage
from google.oauth2 import service_account

PROJECT_ID = os.environ.get("PROJECT_ID", "earthquake-usgs")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "earthquake-usgs_data")
BIGQUERY_DATASET = os.environ.get("GCP_BQ_DATASET", "earthquake_usgs")
TEMP_STORAGE_PATH = os.getenv("TEMP_STORAGE_PATH", "not-found")

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2023, 1, 5)

with DAG(
    dag_id="usgs_earthquake_pipeline",
    description="""Data engineering pipeline to collect, transform, and load earthquake data from USGS website""",
    schedule="0 0 * * *",
    default_args=default_args,
    start_date=START_DATE,
    end_date=END_DATE,
    catchup=True,
    max_active_runs=1,
    tags=["usgs-earthquake"],
) as dag:
    starttime = "{{ ds }}"
    endtime = "{{ next_ds }}"
    api_url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={starttime}&endtime={endtime}"
    LOCAL_JSON = "data/raw/json"

    @task
    def fetch_data(url):
        json_data = requests.get(url).json()
        return json_data

    @task
    def write_json_to_local(path, json_data, file_name):
        with open(f"{path}/{file_name}", "w") as f:
            json.dump(json_data, f)
        return f"{path}/{file_name}"

    json_data = fetch_data(api_url)
    json_local_path = write_json_to_local(LOCAL_JSON, json_data, "data_{{ ds }}.json")
