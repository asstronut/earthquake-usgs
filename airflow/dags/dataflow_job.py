import os
from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "earthquake-usgs")
REGION = os.environ.get("GCP_REGION")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "earthquake-usgs_data")
BQ_DATASET = os.environ.get("GCP_BQ_DATASET", "earthquake_usgs")
SOURCES_PATH = os.getenv("SOURCES_PATH", "not-found")
PY_FILE_NAME = "data_bq_flow.py"
source_file_path = f"{SOURCES_PATH}/{PY_FILE_NAME}"
# gcs_file_uri = f"gs://{BUCKET}/code/{PY_FILE_NAME}"
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2023, 1, 2)

default_args = {
    "depends_on_past": False,
    # "retries": 1,
    # "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="dataflow_earthquake_pipeline_119",
    description="""Data engineering pipeline to collect, transform, and load earthquake data from USGS website""",
    schedule_interval="@daily",
    default_args=default_args,
    start_date=START_DATE,
    end_date=END_DATE,
    catchup=True,
    max_active_runs=1,
    tags=["usgs-earthquake"],
) as dag:
    ds = "{{ ds }}"
    year = "{{ dag_run.logical_date.strftime('%Y') }}"

    start_python_job = BeamRunPythonPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="trans_pipeline_job",
        py_file=source_file_path,
        py_options=[],
        pipeline_options={
            "input": f"gs://{BUCKET}/raw/parquet/data_{ds}.parquet",
            "output": f"{BQ_DATASET}.data{year}beam",
            "project": PROJECT_ID,
            "region": REGION,
            "runner": "DataflowRunner",
            "job_name": f"job-flow-{ds}",
            "staging_location": f"gs://{BUCKET}/staging/",
            "temp_location": f"gs://{BUCKET}/temp/",
        },
        py_requirements=["apache-beam[gcp]==2.52.0"],
        py_interpreter="python3",
        py_system_site_packages=False,
    )

    # start_python_job = DataflowTemplatedJobStartOperator(
    #     template=f"gs://{BUCKET}",
    #     project_id=PROJECT_ID,
    #     task_id="start_template_job",
    #     job_name=f"job-flow-{ds}",
    # )

    start_python_job
