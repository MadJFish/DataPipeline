from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="manual_geo_querier")

# The following works:
# Address with block and street name. Eg. '605 ANG MO KIO AVENUE 5'
# Name of point of interest: 'SUNTEC CITY'
ADDRESSES = [
    'XINMIN PRIMARY SCHOOL',
    'SUNTEC CITY',
    '22 BALAM ROAD'
]

for address in ADDRESSES:
    location = geolocator.geocode(address)
    print('%s \t - \t %s, %s' % (location.address, location.latitude, location.longitude))
