import os
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
)
from google.cloud import storage

PROJECT_ID = os.environ.get("PROJECT_ID", "earthquake-usgs")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "earthquake-usgs_data")
BIGQUERY_DATASET = os.environ.get("GCP_BQ_DATASET", "earthquake_usgs")
REGION = os.environ.get("GCP_REGION")
DATAPROC_CLUSTER = os.environ.get("GCP_DATAPROC_CLUSTER")
SOURCES_PATH = os.getenv("SOURCES_PATH", "not-found")
PY_FILE_NAME = "spark_transforms.py"
source_file_path = f"{SOURCES_PATH}/{PY_FILE_NAME}"
gcs_file_uri = f"gs://{BUCKET}/code/{PY_FILE_NAME}"

default_args = {
    "start_date": datetime(2023, 1, 1),
    "end_date": datetime(2023, 1, 5),
    "depends_on_past": False,
}

with DAG(
    dag_id="dataproc_spark_job_4",
    schedule_interval="0 6 * * *",
    default_args=default_args,
    max_active_runs=1,
    tags=["usgs-earthquake"],
) as dag:
    # @dag.task
    # def upload_to_gcs(local_path, bucket_name, destination_path):
    #     """Upload collected data (json and parquet) to Google Cloud Storage"""
    #     client = storage.Client()
    #     bucket = client.bucket(bucket_name)

    #     blob = bucket.blob(destination_path)
    #     blob.upload_from_filename(local_path)

    # upload_to_gcs(source_file_path, BUCKET, destina)
    ds = "{{ ds }}"
    year = "{{ dag_run.logical_date.strftime('%Y') }}"

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER,
        num_workers=0,
        worker_machine_type="n1-standard-2",
        region=REGION,
    )

    submit_dataproc_job = DataprocSubmitPySparkJobOperator(
        task_id="dataproc_submit_spark_job",
        main=gcs_file_uri,
        arguments=[
            f"--input_data=gs://{BUCKET}/raw/parquet/data_{ds}.parquet",
            f"--output_dt=data{year}",
        ],
        region=REGION,
        cluster_name=DATAPROC_CLUSTER,
        dataproc_jars=[f"gs://{BUCKET}/lib/spark-3.2-bigquery-0.34.0.jar"],
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER,
        region=REGION,
        trigger_rule="all_done",
    )

    create_dataproc_cluster >> submit_dataproc_job >> delete_dataproc_cluster
