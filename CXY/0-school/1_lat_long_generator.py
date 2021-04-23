import sys
import datetime

from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

import pyspark
from pyspark.conf import SparkConf

import pyspark.sql.functions as sql_functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *


# Setting pyspark variables
APP_NAME = "lat_long_generator"  # Any unique name works
INPUT_FILE = "gs://ebd-group-project-data-bucket/0-school/0-external-data/general-information-of-schools.csv"
OUTPUT_FOLDER = "gs://ebd-group-project-data-bucket/0-school/1-wip-data/address_lat_long_ref_table"
counter = [0]
total = 0


# Generate Geo cache
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
sc = pyspark.SparkContext()
geo_locator = Nominatim(user_agent=APP_NAME)
spark = SparkSession.builder.appName(APP_NAME).config(conf=sc.getConf()).getOrCreate()


# Read input
all_df = spark.read.csv(INPUT_FILE, inferSchema=True, header=True)
all_df.printSchema()

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