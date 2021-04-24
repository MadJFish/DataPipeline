import sys
import datetime
from math import radians, cos, sin, asin, sqrt

import pyspark
from pyspark.conf import SparkConf

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import *

APP_NAME = "get_filtered_distance"  # Any unique name works
INPUT_RESALE_FILE = "gs://ebd-group-project-data-bucket/2-nearby-resale/0-external-data/resale_lat_long.csv"
INPUT_SCHOOL_FILE = "gs://ebd-group-project-data-bucket/2-nearby-resale/0-external-data/school_lat_long.csv"
OUTPUT_PARENT_FOLDER = "gs://ebd-group-project-data-bucket/2-nearby-resale/1-wip-data/1_get_filtered_distance"


def get_distance(school_lat_long, resale_lat_long):
    school_array = school_lat_long.strip().split(',')
    resale_array = resale_lat_long.strip().split(',')

    return haversine(float(school_array[0]),
                     float(school_array[1]),
                     float(resale_array[0]),
                     float(resale_array[1]))


# Reference:
# https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


# Set up pyspark and geopy
sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(APP_NAME).config(conf=sc.getConf()).getOrCreate()

# Set up methods for pyspark dataframe
# Reference: https://towardsdatascience.com/5-ways-to-add-a-new-column-in-a-pyspark-dataframe-4e75c2fd8c08
udf_get_distance = sql_functions.udf(get_distance, FloatType()).asNondeterministic()

# Read input
resale_df = spark.read.csv(INPUT_RESALE_FILE, inferSchema=True, header=True)
school_df = spark.read.csv(INPUT_SCHOOL_FILE, inferSchema=True, header=True)

# Get schools as array
school_row_array = school_df.collect()

# Get resales within 3km and export the results for each school respectively
for school_row in school_row_array:
    # Get resales within 3km
    resale_distance_df = resale_df \
        .withColumn('distance', udf_get_distance(lit(school_row['lat_long']), 'lat_long')) \
        .filter("distance BETWEEN 0 AND 3")

    # Export to csv
    output_folder = '%s/%s' % (OUTPUT_PARENT_FOLDER, school_row['school'])
    resale_distance_df \
        .coalesce(1) \
        .write \
        .format('csv') \
        .option('header', 'true') \
        .save(output_folder)
