from google.cloud import datastore, storage
import datetime
import json
from math import floor
import os

GCS_BUCKET = "flatten-staging-271921.appspot.com"
UPLOAD_FILE = "form_data.json"
DS_NAMESPACE =  "flatten_staging"
DS_KIND = "form-user"

def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            data_string, destination_blob_name
        )
    )


def main(message_attributes):
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    map_data = {'time': floor(datetime.datetime.utcnow().timestamp()), 'max': 0, 'fsa': {}}

    query = datastore_client.query(kind=DS_KIND)
    total_responses = 0
    for entity in query.fetch():
        try:
            # make this get the latest form data...
            postcode = entity['form_responses']['postalCode'].upper()
            pot = 1 if entity['probable'] else 0
            risk = 1 if entity['at_risk'] else 0
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
    map_data['total_responses'] = total_responses

    json_str = json.dumps(map_data)

    upload_blob(bucket, json_str, UPLOAD_FILE)


if __name__ == "__main__":
    main(None, None)
