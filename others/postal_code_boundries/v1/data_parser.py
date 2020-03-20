# This script parses the Statistic Canada postal code data into a readable format for the website.
#
# The script reads the XML data from raw_data.gml and outputs a clean JSON file.
# Specifically, Forward Sortation Area (FSA) are the first 3 characters or your postal code.
# The script generates a JSON file that maps FSAs to a list of coordinates representing a polygon or the area.

import xml.etree.ElementTree as ET
import pyproj
import json

USE_REDUCED_DATA_SET = False
FILTER_TO_ONLY_ONTARIO = True
FME_URL = 'http://www.safe.com/gml/fme'
GML_URL = 'http://www.opengis.net/gml'


def prepend_url(url, tag):
    return "{" + url + "}" + tag


PROVINCE_CODE_TAG = prepend_url(FME_URL, 'PRUID')
FEATURE_TAG = prepend_url(GML_URL, 'featureMember')
FSA_CODE_TAG = prepend_url(FME_URL, 'CFSAUID')
POS_LIST_TAG = prepend_url(GML_URL, 'posList')

ONTARIO_CODE = 35

# Number of decimal places to keep in lat-long coordinates
# 4 decimal places gives 11m accuracy according to http://wiki.gis.com/wiki/index.php/Decimal_degrees
ROUNDING_ACCURACY = 4

# The formats used by Stats Can according to https://epsg.io/3347
CONVERT_IN_PROJ = pyproj.Proj('epsg:3347')
CONVERT_OUT_PROJ = pyproj.Proj('epsg:4326')


def read_data():
    file_name = 'reduced_test_data.gml' if USE_REDUCED_DATA_SET else 'raw_data.gml'

    tree = ET.parse(file_name)
    root = tree.getroot()

    # Dictionary returning the data that is read
    output_data = {}
    skipped_data_points = 0

    # For each FSA
    for element in root.findall(FEATURE_TAG):
        province_code = int(element[0].find(PROVINCE_CODE_TAG).text)

        # If in Ontario or filtering not required
        if (not FILTER_TO_ONLY_ONTARIO) or province_code == ONTARIO_CODE:
            # Get data points
            fsa_code = element[0].find(FSA_CODE_TAG).text
            coord_list = element[0][3][0][0][0][0][0][0]

            # Verify that coordinate list is valid
            if coord_list.tag != POS_LIST_TAG:
                print("Problem with data formatting")
                continue

            # Split list of coord into list
            coordinates = coord_list.text.split(" ")

            # Convert to float and pair coordinates
            input_coord = []

            for i in range(len(coordinates)):
                if i % 2 == 0:
                    input_coord.append((float(coordinates[i]), float(coordinates[i + 1])))

            # Convert using pyproj library and add to data structure
            output_coord = []

            for coord in pyproj.itransform(CONVERT_IN_PROJ, CONVERT_OUT_PROJ, input_coord):
                lat = round(coord[0], ROUNDING_ACCURACY)
                lng = round(coord[1], ROUNDING_ACCURACY)

                if len(output_coord) > 0 and output_coord[-1]['lat'] == lat and output_coord[-1]['lng'] == lng:
                    skipped_data_points += 1
                    continue

                output_coord.append({'lat': lat, 'lng': lng})

            output_data[fsa_code] = output_coord

    return output_data, skipped_data_points


def write_data(data_to_write):
    filename = 'reduced_output_data.json' if USE_REDUCED_DATA_SET else 'real_output_data.json'
    with open(filename, 'w') as file:
        json.dump(data_to_write, file)


if __name__ == "__main__":
    data, num_skipped = read_data()
    print(num_skipped)
    write_data(data)
