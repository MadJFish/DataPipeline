import sys
import glob
import logging
import os
import re
from google.cloud import storage

from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

targetSubDirectory = "address_lat_long_ref_table"
outputFileName = "merged.csv"

if len(sys.argv) == 3:
    targetSubDirectory = sys.argv[1]
    outputFileName = sys.argv[2]

APP_NAME = "lat_long_generator"  # Any unique name works
BUCKET_NAME = "ebd-group-project-data-bucket"
GS_DIR = "gs://ebd-group-project-data-bucket/2-nearby-resale/1-wip-data"
LOCAL_DIR = "1-wip-data"
INPUT_FILES = f"{LOCAL_DIR}/{targetSubDirectory}/*/*.csv"
OUTPUT_FILE = f"{LOCAL_DIR}/{outputFileName}.csv"
counter = [0]
bad_records = []

print(INPUT_FILES)

#copy files downt to merge
os.system(f"gsutil -m cp -r {GS_DIR} .")

with open(OUTPUT_FILE, 'w', encoding="cp437") as write_stream:
    is_print_header = True

    for file_name in glob.glob(INPUT_FILES):
        print('File: ', file_name)
        with open(file_name, encoding="cp437") as read_stream:
            header = next(read_stream)

            if is_print_header:
                is_print_header = False
                write_stream.write(header)

            counter[0] = 0
            for line in read_stream:
                write_stream.write(line)


                counter[0] += 1
                if counter[0] % 10 == 0:
                    print('Counted: ', counter[0])
            print('Total: ', counter[0])

