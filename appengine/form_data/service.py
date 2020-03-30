from google.cloud import datastore, storage
import datetime
import google
import json
from math import floor
import os

GCS_BUCKET = os.environ['GCS_BUCKET']
UPLOAD_FILE = 'form_data.json'
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'form-user'

# Downloads a blob from the bucket as a string
def download_blob(bucket_name, source_blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    s = blob.download_as_string()
    return s

def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            data_string, destination_blob_name
        )
    )

def main(get_all):
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    if get_all:
        map_data = {'time': floor(datetime.datetime.utcnow().timestamp()), 'max': 0, 'fsa': {}}

        query = datastore_client.query(kind=DS_KIND)
        total_responses = 0
    else:
        map_data = json.load(download_blob(GCS_BUCKET, 'form_data.json'))
        time = map_data['time']

        query = datastore_client.query(kind=DS_KIND)
        query.add_filter('timestamp', '>', time)

        total_responses = map_data['total_responses']

    for entity in query.fetch():
        try:
            # make this get the latest form data...
            postcode = entity['form_responses']['postalCode'].upper()
            pot = 1 if entity['probable'] else 0
            risk = 1 if entity['at_risk'] else 0
            if (pot == 1 and risk == 1):
                potrisk = 1
                pot = 0
                risk = 0
            else:
                potrisk = 0
            timestamp = entity['timestamp']
        except KeyError as e:
            continue

        total_responses += 1

        if timestamp > time:
            time = timestamp
        
        if postcode in map_data['fsa']:
            map_data['fsa'][postcode]['number_reports'] += 1
            map_data['fsa'][postcode]['pot'] += pot
            map_data['fsa'][postcode]['risk'] += risk
            map_data['fsa'][postcode]['potrisk'] += potrisk
        else:
            map_data['fsa'][postcode] = {'number_reports': 1, 'pot': pot, 'risk': risk, 'potrisk': potrisk}
        map_data['max'] = max(map_data['max'],
                              map_data['fsa'][postcode]['pot'] + 2 * map_data['fsa'][postcode]['risk'])
    map_data['total_responses'] = total_responses
    map_data['time'] = timestamp

    json_str = json.dumps(map_data)

    upload_blob(bucket, json_str, UPLOAD_FILE)