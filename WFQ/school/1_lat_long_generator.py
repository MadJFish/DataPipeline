from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import datetime

from WFQ.abbreviation_ref import expand_street_name

APP_NAME = "lat_long_generator"  # Any unique name works
INPUT_FILE = "external_data/general-information-of-schools.csv"
OUTPUT_FOLDER = "wip_data/address_lat_long_ref_table"
counter = [0]
total = 0


def get_df_filtered_by_years(df, years):
    queries = []
    for year in years:
        queries.append("month LIKE '%s%%'" % year)
    formulated_query = ' OR '.join(queries)
    return df.filter(formulated_query)


def get_address(block, street_name):
    return '%s %s' % (block, expand_street_name(street_name))


def generate_geo_cache(schools):
    school_lat_long_array = []
    for school in schools:
        counter[0] = counter[0] + 1
        if counter[0] % 10 == 0:
            print('%d / %d - %s' % (counter[0], total, school))

        school_lat_long = {
            'school': school,
            'lat_long': 'NONE'
        }

        try:
            location = geo_locator.geocode(school)
            if location is not None:
                returned_address = location.address.upper()
                if 'SINGAPORE' not in returned_address:
                    school_lat_long['lat_long'] = "Error: Not Singapore. Returned address %s" % location.address
                else:
                    if school not in returned_address:
                        if returned_address[0].isdigit():
                            school_lat_long['lat_long'] = "Error: Block retrieved. Returned address %s" % location.address
                        else:
                            school_lat_long['lat_long'] = "Error: Road retrieved. Returned address %s" % location.address
                    else:
                        lat_long = '%s, %s' % (location.latitude, location.longitude)
                        school_lat_long['lat_long'] = lat_long
            else:
                school_lat_long['lat_long'] = 'NONE'
        except GeocoderTimedOut:
            school_lat_long['lat_long'] = "Error: geocode failed on input %s" % school

        school_lat_long_array.append(school_lat_long)
    return school_lat_long_array


# Set up pyspark and geopy
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
geo_locator = Nominatim(user_agent=APP_NAME)

# Set up methods for pyspark dataframe
# Reference: https://towardsdatascience.com/5-ways-to-add-a-new-column-in-a-pyspark-dataframe-4e75c2fd8c08
udf_get_address = sql_functions.udf(get_address, StringType())

# Read input
all_df = spark.read.csv(INPUT_FILE, inferSchema=True, header=True)

# Filter Primary school
primary_df = all_df.filter("mainlevel_code == '%s'" % 'PRIMARY')
primary_df.select('school_name').show(truncate=False)

# Get Total Count
total = primary_df.count()

# Generate dictionary with lat long
school_name_array = [row.school_name for row in primary_df.select('school_name').collect()]
print(school_name_array)
print("start time:-", datetime.datetime.now())
geo_cache = generate_geo_cache(school_name_array)
print(geo_cache)
print('Geo_cache: %d' % len(geo_cache))
print("end time:-", datetime.datetime.now())

# Create external_data frame
geo_cache_df = spark.createDataFrame(geo_cache)
geo_cache_df.show()

# Export address and lat long as csv
geo_cache_df.write \
    .format('csv') \
    .option('header', 'true') \
    .save(OUTPUT_FOLDER)

# # Add column 'address'
# all_df = all_df.withColumn('address', udf_get_address('block', 'street_name'))
#
# # Get unique addresses
# unique_address_df = all_df.select('block', 'street_name', 'address').dropDuplicates(['address'])
#
# # Get Total Count
# total = unique_address_df.count()
# print('Total Count: %d' % total)
#
# # Generate dictionary with lat long
# address_array = [row.address for row in unique_address_df.select('address').collect()]
# print("start time:-", datetime.datetime.now())
# geo_cache_kvp = generate_geo_cache(address_array)
# print('Geo_cache: %d' % len(geo_cache_kvp))
# print("end time:-", datetime.datetime.now())
#
# # Create external_data frame
# geo_cache_df = spark.createDataFrame(geo_cache_kvp.values())
# geo_cache_df.show()
# unique_address_df = unique_address_df.join(geo_cache_df, ['address'])
# unique_address_df.show()
#
# # Export address and lat long as csv
# unique_address_df.write \
#     .format('csv') \
#     .option('header', 'true') \
#     .save(OUTPUT_FOLDER)
