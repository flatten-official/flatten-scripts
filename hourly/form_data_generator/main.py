from google.cloud import datastore, storage
import time
import google
import json
from math import floor

DATASTORE_KIND = 'form-user'

GCS_BUCKET = 'flatten-271620.appspot.com'
UPLOAD_FILE = 'form_data.js'


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

    datastore_client = datastore.Client()

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    query_since = 0
    query_to = floor(time.time())
    try:
        user_map_data_json = download_blob(bucket, UPLOAD_FILE)
        map_data = json.loads(user_map_data_json)
        query_since = map_data['timestamp']
    except google.api_core.exceptions.NotFound:
        map_data = {'max': 0, 'fsa': {}}
    map_data['timestamp'] = query_to

    query = datastore_client.query(kind=DATASTORE_KIND,
                                   # NB time in DB is int ms
                                   filters=(('timestamp', '>=', query_since * 1000),
                                            ('timestamp', '<', query_to * 1000))
                                   )
    for entity in query.fetch():
        try:
            # make this get the latest form data...
            postcode = entity['form_responses']['postcode']
            symptoms = 1 if entity['probable'] else 0
            at_risk = 1 if entity['at_risk'] else 0
        except KeyError as e:
            continue

        if postcode in map_data['fsa']:
            map_data['fsa'][postcode]['number_reports'] += 1
            map_data['fsa'][postcode]['mild'] += symptoms
            map_data['fsa'][postcode]['severe'] += at_risk
        else:
            map_data['fsa'][postcode] = {'number_reports': 1, 'mild': symptoms, 'severe': at_risk}
        map_data['max'] = max(map_data['max'],
                              map_data['fsa'][postcode]['mild'] + 2 * map_data['fsa'][postcode]['severe'])

    json_str = json.dumps(map_data)

    upload_blob(bucket, json_str, UPLOAD_FILE)


if __name__ == "__main__":
    main(None, None)
