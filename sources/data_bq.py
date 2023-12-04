import os
import sys
import typing
from datetime import datetime
from functools import reduce
from apache_beam.io.gcp.internal.clients import bigquery

import apache_beam as beam

PROJECT = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_PROJECT_DATA_LAKE_NAME")
REGION = os.getenv("GCP_PROJECT_REGION")
BQ_DATASET = os.getenv("GCP_PROJECT_BQ_DATASET_NAME")


class ConvertParquetToCSV(beam.DoFn):
    def process(self, element):
        # element = USGSEarthquake(**element)
        csv_element = ",".join(str(field) for field in element.values())
        yield csv_element


class ConvertDataTypes(beam.DoFn):
    @staticmethod
    def convert_timestamp_to_datetime(ts):
        return datetime.fromtimestamp(ts / 1000)

    def process(self, element):
        for c in ["properties_time", "properties_updated"]:
            element[c] = self.convert_timestamp_to_datetime(element[c])

        yield element


class ExtractRegion(beam.DoFn):
    def process(self, element):
        if element["properties_place"] is not None:
            element["region"] = element["properties_place"].split(",")[-1].strip()
        else:
            element["region"] = None
        yield element


class RenameColumns(beam.DoFn):
    def process(self, element):
        new_cols_name = {i: i.replace("properties_", "") for i in element.keys()}
        new_element = {v: element[k] for k, v in new_cols_name.items()}

        yield new_element


def run():
    options = {
        "project": PROJECT,
        "region": REGION,
        "runner": "DataflowRunner",
        "job_name": "example_job_airflow",
        "staging_location": f"gs://{BUCKET}/staging/",
        "temp_location": f"gs://{BUCKET}/temp/",
        "save_main_session": True,
    }
    pipeline_options = beam.pipeline_context.pipeline.PipelineOptions(
        flags=[], **options
    )
    p = beam.Pipeline(options=pipeline_options)
    inp_path = f"gs://{BUCKET}/raw/parquet/*"

    selected_cols = [
        "id",
        "mag",
        "place",
        "time",
        "updated",
        "tz",
        "url",
        "detail",
        "felt",
        "cdi",
        "mmi",
        "alert",
        "status",
        "tsunami",
        "sig",
        "net",
        "code",
        "ids",
        "sources",
        "types",
        "nst",
        "dmin",
        "rms",
        "gap",
        "magType",
        "type",
        "title",
        "geometry_coordinates_latitude",
        "geometry_coordinates_longitude",
        "geometry_coordinates_depth",
        "region",
    ]

    table_schema = {
        "fields": [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "mag", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "place", "type": "STRING", "mode": "NULLABLE"},
            {"name": "time", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "updated", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "tz", "type": "STRING", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
            {"name": "felt", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cdi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mmi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tsunami", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sig", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "net", "type": "STRING", "mode": "NULLABLE"},
            {"name": "code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
            {"name": "types", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "dmin", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rms", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gap", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "geometry_coordinates_latitude",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {
                "name": "geometry_coordinates_longitude",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {
                "name": "geometry_coordinates_depth",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {"name": "region", "type": "STRING", "mode": "NULLABLE"},
        ]
    }

    # pipeline
    pq_data = p | "GetParquet" >> beam.io.ReadFromParquet(inp_path)

    transformed_data = (
        pq_data
        | "ConvertDataTypes" >> beam.ParDo(ConvertDataTypes())
        | "ExtractRegionFromPlace" >> beam.ParDo(ExtractRegion())
        | "RenameColumns" >> beam.ParDo(RenameColumns())
        | "SelectColumns" >> beam.Map(lambda dic: {i: dic[i] for i in selected_cols})
    )

    # transformed_data | "WriteToCsv" >> beam.io.WriteToText(output_prefix)

    table_spec = bigquery.TableReference(
        projectId=PROJECT, datasetId=BQ_DATASET, tableId="data2023beam"
    )

    transformed_data | "LoadToBigQuery" >> beam.io.WriteToBigQuery(
        table=table_spec,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={
            "timePartitioning": {"type": "DAY", "field": "time"},
            "clustering": {"fields": ["region"]},
        },
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
