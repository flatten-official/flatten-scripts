import json
import csv
import os

COMPOSER_DATA_FOLDER = "/home/airflow/gcs/data"

def convert_zip_to_county(map_data_usa, county_dict={}):
    """ Maps zip codes to county codes. """
    fname = 'zip_lookup.json'
    # open file containing zip codes to county mapping
    if os.path.exists(COMPOSER_DATA_FOLDER):
        fname = os.path.join(COMPOSER_DATA_FOLDER, fname)
    with open(fname, 'r') as zipcodes:
        zipcodes_dict = json.load(zipcodes)

    for agg_zip, values in map_data_usa.items():
        try:
            county = zipcodes_dict.get(str(agg_zip))['county_COUNTYNS']
        except TypeError:
            continue
        # ignore if zip code does not exist in dict or if it maps to an empty string
        if not county:
            continue

        if county_dict.get(county):
            county_dict[county]["number_reports"] += values["number_reports"]
            county_dict[county]["pot"] += values["pot"]
            county_dict[county]["risk"] += values["risk"]
            county_dict[county]["both"] += values["both"]
        else:
            county_dict[county] = values
            try:
                del county_dict["fsa_excluded"]
            except KeyError:
                continue
    return county_dict