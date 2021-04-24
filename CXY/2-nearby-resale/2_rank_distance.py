import glob
import re

import pyspark
from pyspark.conf import SparkConf

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from google.cloud import storage

BUCKET_NAME = "ebd-group-project-data-bucket"
APP_NAME = "rank_distance"  # Any unique name works
OUTPUT_PARENT_FOLDER = "gs://ebd-group-project-data-bucket/2-nearby-resale/1-wip-data/2_rank_distance"


def rank_distance(distance):
    if distance > 2:
        return 3
    elif distance > 1:
        return 2
    else:
        return 1


#setup google cloud strage variables
storage_client = storage.Client()
blobs = storage_client.list_blobs(BUCKET_NAME, prefix="2-nearby-resale/1-wip-data/1_get_filtered_distance")


# Set up pyspark and geopy
sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(APP_NAME).config(conf=sc.getConf()).getOrCreate()

# Set up methods for pyspark dataframe
# Reference: https://towardsdatascience.com/5-ways-to-add-a-new-column-in-a-pyspark-dataframe-4e75c2fd8c08
udf_rank_distance = sql_functions.udf(rank_distance, IntegerType())

for blob in blobs:
    file_name = f"gs://{BUCKET_NAME}/{blob.name}"

    if (file_name.endswith(".csv")):
        # Read input
        school_resale_df = spark.read.csv(file_name, inferSchema=True, header=True)
        school_resale_df = school_resale_df \
            .withColumn('rank', udf_rank_distance('distance')) \
            .orderBy('distance')

        # Export to csv
        school_name = re.split('/|\\\\', file_name)[6]
        output_folder = f"{OUTPUT_PARENT_FOLDER}/{school_name}"
        school_resale_df \
            .coalesce(1) \
            .write \
            .format('csv') \
            .option('header', 'true') \
            .save(output_folder)
