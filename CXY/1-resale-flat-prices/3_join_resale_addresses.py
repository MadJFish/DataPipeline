import sys
import datetime

from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

import pyspark
from pyspark.conf import SparkConf

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *

APP_NAME = "join_resales_addresses"  # Any unique name works
YEAR_FILTER = ['2020', '2021']
INPUT_RESALES_FILE = "gs://ebd-group-project-data-bucket/1-resale-flat-prices/0-external-data/test.csv"
INPUT_ADDRESS_FILE = "gs://ebd-group-project-data-bucket/1-resale-flat-prices/1-wip-data/resale_lat_long.csv"
OUTPUT_FOLDER = "gs://ebd-group-project-data-bucket/1-resale-flat-prices/1-wip-data/resales_join_address"
OUTPUT_FILE = "gs://ebd-group-project-data-bucket/1-resale-flat-prices/1-wip-data/resales_join_address.csv"


def get_df_filtered_by_years(df, years):
    queries = []
    for year in years:
        queries.append("month LIKE '%s%%'" % year)
    formulated_query = ' OR '.join(queries)
    return df.filter(formulated_query)


# Set up pyspark and geopy
sc = pyspark.SparkContext()
geo_locator = Nominatim(user_agent=APP_NAME)
spark = SparkSession.builder.appName(APP_NAME).config(conf=sc.getConf()).getOrCreate()

# Read input
resales_df = spark.read.csv(INPUT_RESALES_FILE, inferSchema=True, header=True)
address_df = spark.read.csv(INPUT_ADDRESS_FILE, inferSchema=True, header=True)

# Filter years of resales
resales_df = get_df_filtered_by_years(resales_df, YEAR_FILTER)

# Filter addresses which do not have a lat long
address_df = address_df.filter("lat_long NOT LIKE 'Error%' AND lat_long NOT LIKE 'NONE'")
# address_df.show(truncate=False)

# Join address into resales to get lat_long
joined_df = resales_df.join(address_df, ['block', 'street_name'])

# Export to csv
joined_df\
    .coalesce(1)\
    .write \
    .format('csv') \
    .option('header', 'true') \
    .save(OUTPUT_FOLDER)
