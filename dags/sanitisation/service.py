from google.cloud import datastore, storage
import sanitisation
import io
import os
import time
import csv
from gsc.bucket_functions import upload_blob

GCS_BUCKETS = os.environ['GCS_BUCKETS'].split(',')
GCS_PATHS = os.environ['GCS_PATHS'].split(',')
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'FlattenAccount'
END_FILE_NAME = os.environ['END_FILE_NAME']


def load_excluded_postal_codes(fname="excluded_postal_codes.csv"):
    with open(fname) as csvfile:
        reader = csv.reader(csvfile)
        first_row = next(reader)
    return first_row

def main():
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()

    query = datastore_client.query(kind=DS_KIND)

    excluded = load_excluded_postal_codes()

    sanitisor = sanitisation.Sanitisor(excluded)

    # todo - potentially shift to writing to disk if / when we move off off app engine
    output = csv.StringIO()
    writer = csv.DictWriter(output, fieldnames=sanitisor.field_names)
    writer.writeheader()
    
    for entity in query.fetch():
        l = sanitisor.sanitise_account(entity)
        for obj in l:
            writer.writerow(obj)

    curr_time_ms = str(int(time.time() * 1000))

    for bucket_name, path in zip(GCS_BUCKETS, GCS_PATHS):
        bucket = storage_client.bucket(bucket_name)
        file_name = os.path.join(path, "-".join([curr_time_ms, END_FILE_NAME]))
        upload_blob(bucket, output.getvalue(), file_name)