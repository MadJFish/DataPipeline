ABBREVIATION_KVP = {
    'AVE': 'AVENUE',
    'BT': 'BUKIT',
    'CL': 'CLOSE',
    'CRES': 'CRESCENT',
    'CTRL': 'CENTRAL',
    'C\'WEALTH': 'COMMONWEALTH',
    'DR': 'DRIVE',
    'GDNS': 'GARDENS',
    'HTS': 'HEIGHTS',
    'JLN': 'JALAN',
    'KG': 'KAMPONG',
    'LOR': 'LORONG',
    'NTH': 'NORTH',
    'PK': 'PARK',
    'PL': 'PLACE',
    'RD': 'ROAD',
    'ST': 'STREET',
    'ST.': 'SAINT',
    'STH': 'SOUTH',
    'TER': 'TERRACE',
    'UPP': 'UPPER'
}


# Assume only String as input (No special characters)
def expand_street_name(street_name):
    street_name_array = street_name.split()

    for i, word in enumerate(street_name_array):
        if word in ABBREVIATION_KVP:
            street_name_array[i] = ABBREVIATION_KVP[word]

    return ' '.join(street_name_array)
