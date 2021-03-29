import glob

from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import datetime

from WFQ.abbreviation_ref import expand_street_name

APP_NAME = "join_resales_addresses"  # Any unique name works
YEAR_FILTER = ['2020', '2021']
INPUT_RESALES_FILE = "external_data/resale-flat-prices-based-on-registration-date-from-jan-2017-onwards.csv"
INPUT_ADDRESS_FILE = "cleaned_data/addresses.csv"
OUTPUT_FOLDER = "wip_data/resales_join_address"
OUTPUT_FILE = "wip_data/resales_join_address.csv"


def get_df_filtered_by_years(df, years):
    queries = []
    for year in years:
        queries.append("month LIKE '%s%%'" % year)
    formulated_query = ' OR '.join(queries)
    return df.filter(formulated_query)


# Set up pyspark and geopy
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
geo_locator = Nominatim(user_agent=APP_NAME)

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
