import glob
import re

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, mean, count

APP_NAME = "distance_classifier"  # Any unique name works
INPUT_FILES = 'wip_data/2_rank_distance/*/*.csv'
OUTPUT_PARENT_FOLDER = "wip_data/3_distance_classifier"
CLASSIFICATION_DISTANCE = 500   # In meters. Currently split by per 500 meters.


def classify_distance(classification_distance, distance):
    distance_meters = distance * 1000
    return int(distance_meters / (classification_distance + 1))  # Cater for classification = distance by +1


# Set up pyspark and geopy
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# Set up methods for pyspark dataframe
# Reference: https://towardsdatascience.com/5-ways-to-add-a-new-column-in-a-pyspark-dataframe-4e75c2fd8c08
udf_classify_distance = sql_functions.udf(classify_distance, IntegerType())

# Subdivide resales based on CLASSIFICATION_DISTANCE.
for file_name in glob.glob(INPUT_FILES):
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

    # Export to csv
    school_name = re.split('/|\\\\', file_name)[2]
    output_folder = '%s/%s' % (OUTPUT_PARENT_FOLDER, school_name)
    avg_school_resale_df \
        .coalesce(1) \
        .write \
        .format('csv') \
        .option('header', 'true') \
        .save(output_folder)
