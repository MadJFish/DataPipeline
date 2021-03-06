from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

import glob
import os

APP_NAME = "lat_long_generator"  # Any unique name works
INPUT_FILES = 'wip_data/address_lat_long_ref_table/*.csv'
OUTPUT_FILE = 'wip_data/merged.csv'
counter = [0]


def query_lat_long(address):
    try:
        location = geo_locator.geocode(address)
        if location is not None:
            input_address = address.upper().replace(' ', ', ', 1)
            returned_address = location.address.upper()
            if 'SINGAPORE' not in returned_address:
                return "Error: Not Singapore. Returned address %s" % location.address
            else:
                if input_address not in returned_address:
                    if returned_address[0].isdigit():
                        return "Error: Road retrieved. Returned address %s" % location.address
                    else:
                        return "Error: Block retrieved. Returned address %s" % location.address
                else:
                    return '%s, %s' % (location.latitude, location.longitude)
        else:
            return 'NONE'
    except GeocoderTimedOut:
        return "Error: geocode failed on input %s" % address


def is_correct_location(input_address, returned_location):
    input_address = input_address.upper().replace(' ', ', ', 1)
    if input_address in returned_location.address.upper():
        return True
    else:
        return False


# Set up geopy
geo_locator = Nominatim(user_agent=APP_NAME)

# Remove existing output
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

with open(OUTPUT_FILE, 'w', encoding="cp437") as write_stream:
    address_index = None
    lat_long_index = None
    for file_name in glob.glob(INPUT_FILES):
        print('File: ', file_name)
        with open(file_name, encoding="cp437") as read_stream:
            header = next(read_stream)

            if address_index is None or lat_long_index is None:
                header_array = header.strip().split(',')
                address_index = header_array.index('address')
                lat_long_index = header_array.index('lat_long')
                write_stream.write(header)

            counter[0] = 0
            for line in read_stream:
                line_array = line.strip().split(',')
                if line_array[lat_long_index].startswith('Error: geocode'):
                    tries = 0
                    # Keep querying until there is a proper response.
                    while line_array[lat_long_index].startswith('Error: geocode'):
                        tries += 1
                        line_array[lat_long_index] = query_lat_long(line_array[address_index])
                    line = ','.join(line_array)
                    print('Tried: ', tries, '. ', line)
                write_stream.write(line)

                counter[0] += 1
                if counter[0] % 10 == 0:
                    print('Counted: ', counter[0])
            print('Total: ', counter[0])
