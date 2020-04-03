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
FIELDS = ["id", "date", "fsa", 'probable', 'vulnerable']+QUESTION_FIELDS


def is_vulnerable(form_response):
    return form_response['q4'] or form_response['q5']


def is_probable(form_response):
    if form_response['q3']: return True
    if form_response['q1'] and (form_response['q2'] or form_response['q6']): return True
    if form_response['q6'] and (form_response['q2'] or form_response['q3']): return True
    if form_response['q7']: return True

    return False

def str_from_bool(bl):
    return 'y' if bl else 'n'


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
    probable = str_from_bool(is_probable(form_response))
    vulnerable = str_from_bool(is_vulnerable(form_response))
    fields = [unique_id, day, form_response['postalCode'].upper(), probable, vulnerable]
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
            destination_blob_name, bucket
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
        for form_response in entity['history']:
            if form_response['postalCode'] in excluded:
                continue
            fields = retrieve_fields(entity.key.flat_path[-1], form_response)
            csv_lines.append(",".join(fields))

    curr_time_ms = str(int(time.time() * 1000))
    file_name = os.path.join(FILE_DIRECTORY, "-".join([curr_time_ms, END_FILE_NAME]))

    upload_blob(bucket, "\n".join(csv_lines), file_name)
