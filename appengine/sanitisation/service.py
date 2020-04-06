from google.cloud import datastore, storage
import os
import time
from pytz import utc
import datetime
import csv
import uuid

GCS_BUCKETS = os.environ['GCS_BUCKETS'].split(',')
GCS_PATHS = os.environ['GCS_PATHS'].split(',')
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'form-user'
END_FILE_NAME = os.environ['END_FILE_NAME']

# fields for the generated csv
QUESTION_FIELDS = ["q"+str(n) for n in range(1, 9)]
FIELDS = ["id", "date", "fsa", 'probable', 'vulnerable']+QUESTION_FIELDS


def is_vulnerable(response_bools):
    return response_bools['q4'] or response_bools['q5']


def is_probable(response_bools):
    if response_bools['q3']: return True
    if response_bools['q1'] and (response_bools['q2'] or response_bools['q6']): return True
    if response_bools['q6'] and (response_bools['q2'] or response_bools['q3']): return True
    if response_bools['q7']: return True

    return False

def case_checker(response):
    if response['schema_ver'] == '2':
        pot_case = ((response['q4'] == 'y') 
                    or ('a' in response['q1'] and ('d' in response['q1'] or 'e' in response['q1'] or response['q5'] == 'y')) 
                    or ('d' in response['q1'] and 'e' in response['q1'] and response['q5'] == 'y'))

        vulnerable = (response['q3'] != ['i'] and response['q3'] != []) or 'g' in response['q2'] or 'h' in response['q2']
    else:
        pot_case = (response['q3'] == 'y' or (response['q1'] == 'y' and (response['q2'] == 'y' or response['q6'] == 'y'))
                    or response['q7'] or (response['q6'] == 'y' and (response['q2'] == 'y' or response['q3'] == 'y')))
        vulnerable = response['q4'] == 'y' or response['q5'] == 'y'
    return pot_case, vulnerable

def str_from_bool(bl):
    return 'y' if bl else 'n'


def retrieve_fields(unique_id, form_response):
    """Given an entity from the datastore, generate the list which will be joined to make the fields in the csv."""
    # timestamp is in ms since UNIX origin, so divide by 1000 to get seconds
    timestamp = form_response['timestamp']/1000
    # make a UTC datetime object from the timestamp, convert to a day stamp
    day = utc.localize(
        datetime.datetime.utcfromtimestamp(timestamp)
    ).strftime('%Y-%m-%d')
    
    if form_response['schema_ver'] == '1':
        response_bools = {k: form_response[k] == 'y' for k in QUESTION_FIELDS}
        probable = str_from_bool(is_probable(response_bools))
        vulnerable = str_from_bool(is_vulnerable(response_bools))
    else:
        prob, vuln = case_checker(form_response)
        probable = str_from_bool(prob)
        vulnerable = str_from_bool(vuln)

    fields = [unique_id, day, form_response['fsa'].upper(), probable, vulnerable]
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

    query = datastore_client.query(kind=DS_KIND)

    excluded = load_excluded_postal_codes()

    csv_lines = [",".join(FIELDS)]

    for entity in query.fetch():
        unique_id = str(uuid.uuid4())
        for form_response in entity['users']['Primary']['form_responses']:
            if form_response['fsa'] in excluded:
                continue
            fields = retrieve_fields(unique_id, form_response)
            csv_lines.append(",".join(fields))

    curr_time_ms = str(int(time.time() * 1000))
    csv_string = "\n".join(csv_lines)

    for bucket_name, path in zip(GCS_BUCKETS, GCS_PATHS):
        bucket = storage_client.bucket(bucket_name)
        file_name = os.path.join(path, "-".join([curr_time_ms, END_FILE_NAME]))
        upload_blob(bucket, csv_string, file_name)
