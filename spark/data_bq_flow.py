import logging
from datetime import datetime
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import _BeamArgumentParser, PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


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


class TransformOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser: _BeamArgumentParser) -> None:
        parser.add_value_provider_argument(
            "--input",
            default="gs://earthquake-usgs_data/raw/parquet/data_2023-01-01.parquet",
            help="Path of the file to read from",
        )
        parser.add_argument(
            "--output", required=True, help="Big Query table to store transformed data"
        )


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "--input",
    #     default="gs://earthquake-usgs_data/raw/parquet/data_2023-01-01.parquet",
    # )
    # parser.add_argument(
    #     "--output", required=True, help="Big Query table to store transformed data"
    # )

    args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

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

    with beam.Pipeline(options=pipeline_options) as p:
        transform_options = pipeline_options.view_as(TransformOptions)
        # start pipeline
        pq_data = p | "GetParquet" >> beam.io.ReadFromParquet(transform_options.input)

        transformed_data = (
            pq_data
            | "ConvertDataTypes" >> beam.ParDo(ConvertDataTypes())
            | "ExtractRegionFromPlace" >> beam.ParDo(ExtractRegion())
            | "RenameColumns" >> beam.ParDo(RenameColumns())
            | "SelectColumns"
            >> beam.Map(lambda dic: {i: dic[i] for i in selected_cols})
        )

        transformed_data | "LoadToBigQuery" >> beam.io.WriteToBigQuery(
            table=transform_options.output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={
                "timePartitioning": {"type": "DAY", "field": "time"},
                "clustering": {"fields": ["region"]},
            },
        )
        # end of pipeline


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
