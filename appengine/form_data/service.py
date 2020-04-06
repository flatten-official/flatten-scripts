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
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'form-user'


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
            data_string, destination_blob_name
        )
    )


def case_checker(response):
    if response['schema_ver'] == '2':
        pot_case = ((response['q4'] == 'y') 
                    or ('fever' in response['q1'] and ('cough' in response['q1'] or 'shortnessOfBreath' in response['q1'] or response['q5'] == 'y')) 
                    or ('cough' in response['q1'] and 'shortnessOfBreath' in response['q1'] and response['q5'] == 'y'))

        vulnerable = (response['q3'] != ['other'] and response['q3'] != []) or '65-74' in response['q2'] or '>75' in response['q2']
    else:
        pot_case = (response['q3'] == 'y' or (response['q1'] == 'y' and (response['q2'] == 'y' or response['q6'] == 'y'))
                    or response['q7'] or (response['q6'] == 'y' and (response['q2'] == 'y' or response['q3'] == 'y')))
        vulnerable = response['q4'] == 'y' or response['q5'] == 'y'
    
    pot_vuln = 1 if (pot_case and vulnerable) else 0
    pot_case = 1 if pot_case else 0
    vulnerable = 1 if vulnerable else 0
    return pot_case, vulnerable, pot_vuln
        
def main():
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()

    map_data = {'time': floor(datetime.datetime.utcnow().timestamp()), 'max': 0, 'fsa': {}}

    excluded = load_excluded_postal_codes()

    query = datastore_client.query(kind=DS_KIND)
    total_responses = 0
    for entity in query.fetch():
        total_responses += 1
        try:
            response = entity['users']['Primary']['form_responses'][-1]
            postcode = response['fsa']
            pot, risk, both = case_checker(response)
        except KeyError as e:
            continue

        total_responses += 1

        if postcode in map_data['fsa']:
            map_data['fsa'][postcode]['number_reports'] += 1
            if postcode in excluded:
                continue
            map_data['fsa'][postcode]['pot'] += pot
            map_data['fsa'][postcode]['risk'] += risk
            map_data['fsa'][postcode]['both'] += both
        else:
            if postcode in excluded:
                map_data['fsa'][postcode] = {'fsa_excluded': True, 'number_reports': 1}
                continue
            map_data['fsa'][postcode] = {'number_reports': 1, 'pot': pot, 'risk': risk, 'both': both, 'fsa_excluded': False}
        map_data['max'] = max(map_data['max'],
                              map_data['fsa'][postcode]['pot'] + 2 * map_data['fsa'][postcode]['risk'])
    map_data['total_responses'] = total_responses

    json_str = json.dumps(map_data)

    for bucket, path in zip(GCS_BUCKETS, GCS_PATHS):
        bucket = storage_client.bucket(bucket)
        file_path = os.path.join(path, UPLOAD_FILE)
        upload_blob(bucket, json_str, file_path)
