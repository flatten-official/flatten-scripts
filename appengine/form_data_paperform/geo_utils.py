import json


def convert_zip_to_county(map_data_usa):
    """ Maps zip codes to county codes. """
    # open file containing zip codes to county mapping
    with open('zip_lookup.json', 'r') as zipcodes:
        zipcodes_dict = json.load(zipcodes)

    county_dict = {}
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
            county_dict["county_excluded"] = False # for the moment all of these are
            try:
                del county_dict["fsa_excluded"]
            except KeyError:
                continue

    return county_dict


def load_excluded_postal_codes(fname="excluded_postal_codes.csv"):
    with open(fname) as csvfile:
        reader = csv.reader(csvfile)
        first_row = next(reader)
    return first_row
