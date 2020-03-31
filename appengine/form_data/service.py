from google.cloud import datastore, storage
import datetime
import google
import json
from math import floor
import os

GCS_BUCKET = os.environ['GCS_BUCKET']
UPLOAD_FILE = 'form_data.json'
UPLOAD_CASE_TREND = 'case_trend.json'
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'form-user'


def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            data_string, destination_blob_name
        )
    )

# Downloads a blob from the bucket as a string
def download_blob(bucket_name, source_blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    s = blob.download_as_string()
    return s

def case_trend(get_all):
    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    query = datastore_client.query(kind=DS_KIND)
    if get_all:
        case_data = {'time': 0, 'fsa': {}}
    else:
        case_data = json.loads(download_blob(GCS_BUCKET, 'case_trend.json'))
        query.add_filter('timestamp', '>', case_data['time']*1000)

    max_timestamp = case_data['time']
    for entity in query.fetch():
        try:
            # make this get the latest form data...
            postcode = entity['form_responses']['postalCode'].upper()
            timestamp = entity['timestamp'] / 1000
            date = datetime.date.fromtimestamp(timestamp).strftime("%Y-%m-%d")
            pot = 1 if entity['probable'] else 0
        except KeyError as e:
            continue
        if timestamp > max_timestamp:
            max_timestamp = timestamp
        
        if postcode in case_data['fsa']:
            if date in  case_data['fsa'][postcode]:
                case_data['fsa'][postcode][date] += pot
            else:
                case_data['fsa'][postcode][date] = pot 
        else:
             case_data['fsa'][postcode] = {date: pot}

    case_data['time'] = floor(max_timestamp)

    json_str = json.dumps(case_data)
    upload_blob(bucket, json_str, UPLOAD_CASE_TREND)




def main(get_all):
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    query = datastore_client.query(kind=DS_KIND)
    total_responses = 0

    if get_all:
        time = 0
        case_data = {'time': 0, 'fsa': {}}
        map_data = {'time': floor(datetime.datetime.utcnow().timestamp()), 'max': 0, 'fsa': {}}
    else:
        case_data = json.loads(download_blob(GCS_BUCKET, UPLOAD_CASE_TREND))
        map_data = json.loads(download_blob(GCS_BUCKET, UPLOAD_FILE))
        time = min(map_data['time']*1000, case_data['time']*1000)
        query.add_filter('timestamp', '>', time)
    
    max_timestamp = time
    for entity in query.fetch():
        try:
            # make this get the latest form data...
            postcode = entity['form_responses']['postalCode'].upper()
            pot = 1 if entity['probable'] else 0
            risk = 1 if entity['at_risk'] else 0
            timestamp = entity['timestamp'] / 1000
            date = datetime.date.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        except KeyError as e:
            continue
        total_responses += 1

        if postcode in map_data['fsa']:
            map_data['fsa'][postcode]['number_reports'] += 1
            map_data['fsa'][postcode]['pot'] += pot
            map_data['fsa'][postcode]['risk'] += risk
        else:
            map_data['fsa'][postcode] = {'number_reports': 1, 'pot': pot, 'risk': risk}
        map_data['max'] = max(map_data['max'],
                              map_data['fsa'][postcode]['pot'] + 2 * map_data['fsa'][postcode]['risk'])

        if timestamp > max_timestamp:
            max_timestamp = timestamp
        
        if postcode in case_data['fsa']:
            if date in  case_data['fsa'][postcode]:
                case_data['fsa'][postcode][date] += pot
            else:
                case_data['fsa'][postcode][date] = pot 
        else:
             case_data['fsa'][postcode] = {date: pot}

    map_data['time'] = floor(max_timestamp)
    map_data['total_responses'] = total_responses
    case_data['time'] = floor(max_timestamp)

    case_json = json.dumps(case_data)
    map_json = json.dumps(map_data)

    upload_blob(bucket, map_json, UPLOAD_FILE)
    upload_blob(bucket, case_json, UPLOAD_CASE_TREND)
