from google.cloud import datastore, storage
import datetime
import google
import json
from math import floor
import os

GCS_BUCKET = os.environ['GCS_BUCKET']
UPLOAD_FILE = os.environ['UPLOAD_FILE']
DS_NAMESPACE =  os.environ['DS_NAMESPACE']
DS_KIND = os.environ['DS_KIND']


def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            data_string, destination_blob_name
        )
    )


def download_blob(bucket, destination_file_name):
    """Downloads a blob from the bucket."""

    blob = bucket.blob(destination_file_name)

    data_string = blob.download_as_string()

    print(
        "File {} downloaded from {}.".format(
            data_string, destination_file_name
        )
    )

    return data_string


def main(event, context):
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    query_since = 0
    query_to = floor(datetime.datetime.utcnow().timestamp())
    try:
        user_map_data_json = download_blob(bucket, UPLOAD_FILE)
        map_data = json.loads(user_map_data_json)
        query_since = float(map_data['time'])
    except google.api_core.exceptions.NotFound:
        map_data = {'max': 0, 'fsa': {}}
    map_data['time'] = query_to

    query = datastore_client.query(kind=DS_KIND,
                                   # NB time in DB is int ms
                                   filters=(('timestamp', '>=', int(query_since * 1000)),
                                            ('timestamp', '<', int(query_to * 1000)))
                                   )
    for entity in query.fetch():
        try:
            # make this get the latest form data...
            postcode = entity['form_responses']['postalCode']
            pot = 1 if entity['probable'] else 0
            risk = 1 if entity['at_risk'] else 0
        except KeyError as e:
            print(e)
            continue

        if postcode in map_data['fsa']:
            map_data['fsa'][postcode]['number_reports'] += 1
            map_data['fsa'][postcode]['pot'] += pot
            map_data['fsa'][postcode]['risk'] += risk
        else:
            map_data['fsa'][postcode] = {'number_reports': 1, 'pot': pot, 'risk': risk}
        map_data['max'] = max(map_data['max'],
                              map_data['fsa'][postcode]['pot'] + 2 * map_data['fsa'][postcode]['risk'])

    json_str = json.dumps(map_data)

    upload_blob(bucket, json_str, UPLOAD_FILE)


if __name__ == "__main__":
    main(None, None)
