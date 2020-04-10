from google.cloud import datastore, storage
import datetime
import google
import json
from math import floor
import os
import csv

GCS_BUCKETS = os.environ['GCS_BUCKETS'].split(',')
GCS_PATHS = os.environ['GCS_PATHS'].split(',')
UPLOAD_FILE = 'form_data.json'
UPLOAD_FILE_USA = 'form_data_usa.json'
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'FlattenAccount'


def load_excluded_postal_codes(fname="excluded_postal_codes.csv"):
    with open(fname) as csvfile:
        reader = csv.reader(csvfile)
        first_row = next(reader)
    return first_row


def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
             destination_blob_name, bucket
        )
    )


def case_checker(response):
    if response['schema_ver'] == '2':
        pot_case = ((response['contactWithIllness'] == 'y') 
                    or ('fever' in response['symptoms'] and ('cough' in response['symptoms'] or 'shortnessOfBreath' in response['symptoms'] or response['travelOutsideCanada'] == 'y')) 
                    or ('cough' in response['symptoms'] and 'shortnessOfBreath' in response['symptoms'] and response['travelOutsideCanada'] == 'y'))

        vulnerable = (response['conditions'] != ['other'] and response['conditions'] != []) or '65-74' in response['age'] or '>75' in response['age']
    else:
        pot_case = (response['q3'] == 'y' or (response['q1'] == 'y' and (response['q2'] == 'y' or response['q6'] == 'y'))
                    or (response['q7'] == 'y') or (response['q6'] == 'y' and (response['q2'] == 'y' or response['q3'] == 'y')))
        vulnerable = response['q4'] == 'y' or response['q5'] == 'y'
    
    pot_vuln = 1 if (pot_case and vulnerable) else 0
    pot_case = 1 if pot_case else 0
    vulnerable = 1 if vulnerable else 0
    return pot_case, vulnerable, pot_vuln


def convert_zip_to_county(map_data_usa):
    # open file containing zip codes to county mapping
    with open('zip_lookup.json', 'r') as zipcodes:
        zipcodes_dict = json.load(zipcodes)

    county_dict = {}
    for aggregate_fsa, values in map_data_usa.items():
        county = zipcodes_dict.get(str(aggregate_fsa))['county_COUNTYFP']
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
        
def main():
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()

    map_data = {'time': 0, 'total_responses': 0, 'fsa': {}}
    map_data_usa = {'time': 0, 'total_responses': 0, 'fsa': {}}

    excluded = load_excluded_postal_codes()

    query = datastore_client.query(kind=DS_KIND)

    for entity in query.fetch():

        try:
            response = entity['users']['Primary']['form_responses'][-1]
            if 'postalCode' in response:
                postcode = response['postalCode'].upper()
                mp = map_data
            elif 'zipCode' in response:
                postcode = response['zipCode'].upper()
                mp = map_data_usa
            else:
                continue
            pot, risk, both = case_checker(response)
        except (KeyError, IndexError, ValueError) as e:
            continue

        mp['total_responses'] += 1

        if postcode in map_data['fsa']:
            mp['fsa'][postcode]['number_reports'] += 1
            if postcode in excluded:
                continue
            mp['fsa'][postcode]['pot'] += pot
            mp['fsa'][postcode]['risk'] += risk
            mp['fsa'][postcode]['both'] += both
        else:
            if postcode in excluded:
                mp['fsa'][postcode] = {'fsa_excluded': True, 'number_reports': 1}
                continue
            mp['fsa'][postcode] = {'number_reports': 1, 'pot': pot, 'risk': risk, 'both': both, 'fsa_excluded': False}

        mp['time'] = max(mp['time'], entity['created']//1000)  

    json_str = json.dumps(map_data)

    map_data_usa = {
        'time': map_data_usa['time'],
        'total_responses': map_data_usa['total_responses'],
        'county': convert_zip_to_county(map_data_usa['fsa'])
    }
    print(map_data_usa)
    json_str_usa = json.dumps(map_data_usa)

    for bucket, path in zip(GCS_BUCKETS, GCS_PATHS):
        bucket = storage_client.bucket(bucket)
        file_path = os.path.join(path, UPLOAD_FILE)
        file_path_usa = os.path.join(path, UPLOAD_FILE_USA)
        upload_blob(bucket, json_str, file_path)
        upload_blob(bucket, json_str_usa, file_path_usa)
