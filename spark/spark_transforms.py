import os
import argparse
from functools import reduce

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

start_date = None
end_date = None

# Spark session in local
# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# Spark session for Dataproc
spark = SparkSession.builder.master("yarn").appName("earthquakeUsgs").getOrCreate()

# BUCKET = "earthquake-usgs_data"
spark.conf.set("temporaryGcsBucket", "earthquake-usgs_data")

parser = argparse.ArgumentParser()

parser.add_argument("--input_data", required=True)
parser.add_argument("--output_dt", required=True)

args = parser.parse_args()

input_data = args.input_data
output_dt = args.output_dt


SELECTED_COLUMNS = [
    "id",
    "properties_mag",
    "properties_place",
    "properties_time",
    "properties_updated",
    "properties_tz",
    "properties_url",
    "properties_detail",
    "properties_felt",
    "properties_cdi",
    "properties_mmi",
    "properties_alert",
    "properties_status",
    "properties_tsunami",
    "properties_sig",
    "properties_net",
    "properties_code",
    "properties_ids",
    "properties_sources",
    "properties_types",
    "properties_nst",
    "properties_dmin",
    "properties_rms",
    "properties_gap",
    "properties_magType",
    "properties_type",
    "properties_title",
    "geometry_coordinates_latitude",
    "geometry_coordinates_longitude",
    "geometry_coordinates_depth",
    "region",
]


def transform(input_data, dt):
    df = spark.read.option("inferSchema", "true").parquet(input_data)
    # Split the properties_place column
    split_col = F.split(F.col("properties_place"), ",")

    # Get the last item of the split column
    last_item = split_col.getItem(F.size(split_col) - 1)

    df = (
        df.withColumn(
            "properties_time", F.to_timestamp(F.col("properties_time") / 1000)
        )
        .withColumn(
            "properties_updated", F.to_timestamp(F.col("properties_updated") / 1000)
        )
        .withColumn(
            "region",
            F.when(F.col("properties_place").isNotNull(), last_item).otherwise(None),
        )
        .select(SELECTED_COLUMNS)
    )

    new_columns = {i: i.replace("properties_", "") for i in df.columns}

    df = reduce(
        lambda df, idx: df.withColumnRenamed(
            list(new_columns.keys())[idx], list(new_columns.values())[idx]
        ),
        range(len(new_columns)),
        df,
    )

    # df.write.mode("append").parquet()
    df.write.format("bigquery").mode("append").option("partitionField", "time").option(
        "partitionType", "DAY"
    ).option("clusteredField", "region").option("project", "earthquake-usgs").option(
        "dataset", "earthquake_usgs"
    ).option(
        "table", dt
    ).save()


transform(input_data, output_dt)
