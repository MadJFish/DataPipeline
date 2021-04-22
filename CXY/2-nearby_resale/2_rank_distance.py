import glob
import re

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *

APP_NAME = "rank_distance"  # Any unique name works
INPUT_FILES = 'wip_data/1_get_filtered_distance/*/*.csv'
OUTPUT_PARENT_FOLDER = "wip_data/2_rank_distance"


def rank_distance(distance):
    if distance > 2:
        return 3
    elif distance > 1:
        return 2
    else:
        return 1


# Set up pyspark and geopy
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# Set up methods for pyspark dataframe
# Reference: https://towardsdatascience.com/5-ways-to-add-a-new-column-in-a-pyspark-dataframe-4e75c2fd8c08
udf_rank_distance = sql_functions.udf(rank_distance, IntegerType())

for file_name in glob.glob(INPUT_FILES):
    # Read input
    school_resale_df = spark.read.csv(file_name, inferSchema=True, header=True)
    school_resale_df = school_resale_df \
        .withColumn('rank', udf_rank_distance('distance')) \
        .orderBy('distance')

    # Export to csv
    school_name = re.split('/|\\\\', file_name)[2]
    output_folder = '%s/%s' % (OUTPUT_PARENT_FOLDER, school_name)
    school_resale_df \
        .coalesce(1) \
        .write \
        .format('csv') \
        .option('header', 'true') \
        .save(output_folder)
