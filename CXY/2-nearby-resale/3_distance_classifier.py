import glob
import re

import pyspark
from pyspark.conf import SparkConf

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, mean, count

from google.cloud import storage



BUCKET_NAME = "ebd-group-project-data-bucket"
APP_NAME = "distance_classifier"  # Any unique name works
OUTPUT_PARENT_FOLDER = "gs://ebd-group-project-data-bucket/2-nearby-resale/1-wip-data/3_distance_classifier"
CLASSIFICATION_DISTANCE = 500   # In meters. Currently split by per 500 meters.


def classify_distance(classification_distance, distance):
    distance_meters = distance * 1000
    return int(distance_meters / (classification_distance + 1))  # Cater for classification = distance by +1


#setup google cloud strage variables
storage_client = storage.Client()
blobs = storage_client.list_blobs(BUCKET_NAME, prefix="2-nearby-resale/1-wip-data/2_rank_distance")

# Set up pyspark and geopy
sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(APP_NAME).config(conf=sc.getConf()).getOrCreate()

# Set up methods for pyspark dataframe
# Reference: https://towardsdatascience.com/5-ways-to-add-a-new-column-in-a-pyspark-dataframe-4e75c2fd8c08
udf_classify_distance = sql_functions.udf(classify_distance, IntegerType())

# Subdivide resales based on CLASSIFICATION_DISTANCE.
for blob in blobs:
    file_name = f"gs://{BUCKET_NAME}/{blob.name}"
    
    if (file_name.endswith(".csv")):
        print(f"CHHHHHHEEEECCCCKKKK: {file_name}")
        # Read input
        school_resale_df = spark.read.csv(file_name, inferSchema=True, header=True)

        school_resale_df = school_resale_df \
            .withColumn('classification', udf_classify_distance(lit(CLASSIFICATION_DISTANCE), 'distance'))

        avg_school_resale_df = school_resale_df \
            .orderBy('classification') \
            .groupby('classification') \
            .agg(
                mean('distance').alias('avg_distance'),
                mean('resale_price').alias('avg_resale_price'),
                count('*').alias('count')
            )

        # # Export to csv
        # school_name = re.split('/|\\\\', file_name)[2]
        # output_folder = '%s/%s' % (OUTPUT_PARENT_FOLDER, school_name)
        # avg_school_resale_df \
        #     .coalesce(1) \
        #     .write \
        #     .format('csv') \
        #     .option('header', 'true') \
        #     .save(output_folder)
