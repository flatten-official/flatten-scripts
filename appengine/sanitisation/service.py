from google.cloud import datastore, storage
import os
import time
from pytz import utc
import datetime
import csv
from hashlib import sha256

GCS_BUCKET = os.environ['GCS_BUCKET']
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'form-user'
END_FILE_NAME = os.environ['END_FILE_NAME']
FILE_DIRECTORY = os.environ['FILE_DIRECTORY']

# fields for the generated csv
QUESTION_FIELDS = ["q"+str(n) for n in range(1, 9)]
FIELDS = ["id", "date", "fsa"]+QUESTION_FIELDS


def retrieve_fields(key, form_response):
    """Given an entity from the datastore, generate the list which will be joined to make the fields in the csv."""
    # Hash the unique id so that you can't ID by looking at cookies and such, for those entries that have a
    # cookie as the key
    unique_id = sha256(str.encode(key)).hexdigest()
    # timestamp is in ms since UNIX origin, so divide by 1000 to get seconds
    timestamp = form_response['timestamp']/1000
    # make a UTC datetime object from the timestamp, convert to a day stamp
    day = utc.localize(
        datetime.datetime.utcfromtimestamp(timestamp)
    ).strftime('%Y-%m-%d')
    fields = [unique_id, day, form_response['postalCode'].upper()]
    for field in QUESTION_FIELDS:
        try:
            fields.append(form_response[field])
        except KeyError:
            fields.append('')
    return fields


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

def main():
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    query = datastore_client.query(kind=DS_KIND)

    excluded = load_excluded_postal_codes()

    csv_lines = [",".join(FIELDS)]

    for entity in query.fetch():
        if entity['form_responses']['postalCode'] in excluded:
            continue
        for form_response in entity['history']:
            fields = retrieve_fields(entity.key.flat_path[-1], form_response)
            csv_lines.append(",".join(fields))

    curr_time_ms = str(int(time.time() * 1000))
    file_name = os.path.join(FILE_DIRECTORY, "-".join([curr_time_ms, END_FILE_NAME]))

    upload_blob(bucket, "\n".join(csv_lines), file_name)
