from google.cloud import datastore, storage
import json
import os
import csv

from case_checker import case_checker
from geo_utils import convert_zip_to_county, load_excluded_postal_codes

# bucket used to store the old data from when we rolled our own forms
OLD_DATA_BUCKET = os.environ['OLD_DATA_BUCKET']
GCS_BUCKETS = os.environ['GCS_BUCKETS'].split(',')
GCS_PATHS = os.environ['GCS_PATHS'].split(',')
UPLOAD_FILE = 'form_data.json'
UPLOAD_FILE_USA = 'form_data_usa.json'

OLD_FILE = 'form_data_old.json'
OLD_FILE_USA = 'form_data_usa_old.json'

DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'PaperformSubmission'


def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
             destination_blob_name, bucket
        )
    )


def download_blob(bucket, source_blob_name):
    """Downloads a file from the bucket from a string."""

    blob = bucket.get_blob(source_blob_name)

    return blob.download_as_string()

def main():
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()

    bucket = storage_client.bucket(OLD_DATA_BUCKET)
    try:
        map_data = json.loads(download_blob(bucket, OLD_FILE))
        map_data_usa = json.loads(download_blob(bucket, OLD_FILE_USA))
        map_data_usa['zipcode'] = {}
    except Exception as e:
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        map_data = {'time': 0, 'total_responses': 0, 'fsa': {}}
        map_data_usa = {'time': 0, 'total_responses': 0, 'zipcode': {}}

    excluded = load_excluded_postal_codes()

    query = datastore_client.query(kind=DS_KIND)

    for entity in query.fetch():

        try:
            response = entity['data']
            if 'fsa' in response:
                postcode = response['fsa']['value'].upper()
                mp = map_data
                country = 'ca'
            elif 'zip' in response:
                postcode = response['zip']['value'].upper()
                mp = map_data_usa
                country = 'us'
            else:
                continue
            pot, risk, both = case_checker(response)
        except (KeyError, IndexError, ValueError) as e:
            continue

        mp['total_responses'] += 1

        pc_key = 'fsa' if country is 'ca' else 'zipcode'

        if postcode in map_data['fsa']:
            mp[pc_key][postcode]['number_reports'] += 1
            if postcode in excluded:
                continue
            mp[pc_key][postcode]['pot'] += pot
            mp[pc_key][postcode]['risk'] += risk
            mp[pc_key][postcode]['both'] += both
        else:
            if postcode in excluded:
                mp[pc_key][postcode] = {'fsa_excluded': True, 'number_reports': 1}
                continue
            mp[pc_key][postcode] = {'number_reports': 1, 'pot': pot, 'risk': risk, 'both': both, 'fsa_excluded': False}

        mp['time'] = max(mp['time'], entity['timestamp']//1000)  

    json_str = json.dumps(map_data)

    map_data_usa = {
        'time': map_data_usa['time'],
        'total_responses': map_data_usa['total_responses'],
        'county': convert_zip_to_county(map_data_usa['zipcode'], county_dict=map_data_usa['county'])
    }
    json_str_usa = json.dumps(map_data_usa)

    for bucket, path in zip(GCS_BUCKETS, GCS_PATHS):
        bucket = storage_client.bucket(bucket)
        file_path = os.path.join(path, UPLOAD_FILE)
        file_path_usa = os.path.join(path, UPLOAD_FILE_USA)
        upload_blob(bucket, json_str, file_path)
        upload_blob(bucket, json_str_usa, file_path_usa)
