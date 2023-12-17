import os
from datetime import datetime, timedelta
import requests
import json
import pandas as pd

from airflow import DAG
from google.cloud import storage
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "earthquake-usgs")
REGION = os.environ.get("GCP_REGION")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "earthquake-usgs_data")
BIGQUERY_DATASET = os.environ.get("GCP_BQ_DATASET", "earthquake_usgs")
TEMP_STORAGE_PATH = os.getenv("TEMP_STORAGE_PATH", "not-found")

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}
START_DATE = datetime(2020, 5, 1)
END_DATE = datetime(2020, 6, 15)

with DAG(
    dag_id=f"usgs_earthquake_pipeline_{START_DATE.strftime('%Y%m%d')}",
    description="""Data engineering pipeline to collect, transform, and load earthquake data from USGS website""",
    schedule="0 0 1,15 * *",
    default_args=default_args,
    start_date=START_DATE,
    end_date=END_DATE,
    catchup=True,
    max_active_runs=1,
    tags=["usgs-earthquake"],
) as dag:
    starttime = "{{ ds }}"
    endtime = "{{ next_ds }}"
    year = "{{ dag_run.logical_date.strftime('%Y') }}"

    api_url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={starttime}&endtime={endtime}"

    file_name = "data_{{ ds }}"

    LOCAL_JSON_FOLDER = "data/raw/json"
    LOCAL_PARQUET_FOLDER = "data/raw/parquet"
    destination_json = f"raw/json/{file_name}.json"
    destination_parquet = f"raw/parquet/{file_name}.parquet"

    @dag.task
    def fetch_data(url):
        """Pull JSON data from API"""
        json_data = requests.get(url).json()
        return json_data

    @dag.task
    def save_json_to_local(json_data, folder_path, file_name):
        """Save json data to local host"""
        local_path = f"{folder_path}/{file_name}.json"
        with open(local_path, "w") as f:
            json.dump(json_data, f)
        return local_path

    @dag.task
    def save_json_as_pq(json_data, folder_path, file_name):
        """Normalize JSON data and save data as parquet file"""
        df_json_data = pd.json_normalize(
            json_data, record_path=["features"], meta="metadata", sep="_"
        )

        df_json_data[
            [
                "geometry_coordinates_latitude",
                "geometry_coordinates_longitude",
                "geometry_coordinates_depth",
            ]
        ] = df_json_data["geometry_coordinates"].tolist()
        df_json_data.drop(["geometry_coordinates"], axis=1, inplace=True)

        df_json_data = pd.concat(
            [
                df_json_data,
                pd.json_normalize(df_json_data["metadata"]).add_prefix("metadata_"),
            ],
            axis=1,
        )
        df_json_data.drop(["metadata"], axis=1, inplace=True)

        df_json_data[["properties_felt", "properties_nst"]] = df_json_data[
            ["properties_felt", "properties_nst"]
        ].astype("Int64")

        local_path = f"{folder_path}/{file_name}.parquet"
        df_json_data.to_parquet(local_path, compression="gzip")

        return local_path

    @dag.task
    def upload_to_gcs(local_path, bucket_name, destination_path):
        """Upload collected data (json and parquet) to Google Cloud Storage"""
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        blob = bucket.blob(destination_path)
        blob.upload_from_filename(local_path)

        return local_path

    @dag.task
    def delete_local_file(*file_paths):
        for file_path in file_paths:
            if os.path.isfile(file_path):
                os.remove(file_path)
            else:
                print(f"Error: {file_path} not found")

    start_python_job = DataflowTemplatedJobStartOperator(
        template=f"gs://{BUCKET}/templates/TransformData",
        job_name=f"job-flow-{starttime}",
        task_id="start_dataflow_template_job",
        parameters={
            "input": f"gs://{BUCKET}/raw/parquet/data_{starttime}.parquet",
            "output": f"{BIGQUERY_DATASET}.earthquake{year}",
        },
        dataflow_default_options={
            "project": PROJECT_ID,
            "region": REGION,
            "runner": "DataflowRunner",
            "staging_location": f"gs://{BUCKET}/staging/",
            "temp_location": f"gs://{BUCKET}/temp/",
        },
    )

    json_data = fetch_data(api_url)

    json_local_path = save_json_to_local(json_data, LOCAL_JSON_FOLDER, file_name)

    pq_local_path = save_json_as_pq(json_data, LOCAL_PARQUET_FOLDER, file_name)

    json_local_path_up = upload_to_gcs(json_local_path, BUCKET, destination_json)
    pq_local_path_up = upload_to_gcs(pq_local_path, BUCKET, destination_parquet)

    local_file_deleted = delete_local_file(json_local_path_up, pq_local_path_up)
    local_file_deleted >> start_python_job
