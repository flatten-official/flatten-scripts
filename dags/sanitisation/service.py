from google.cloud import datastore, storage
import sanitisation.sanitisation
import io
import json
import os
import time
import csv
from utils.file_utils import load_excluded_postal_codes, load_keys

def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            destination_blob_name, bucket
        )
    )

GCS_BUCKETS = os.environ['GCS_BUCKETS'].split(',')
GCS_PATHS = os.environ['GCS_PATHS'].split(',')
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'FlattenAccount'
DS_KIND_PAPERFORM = 'PaperformSubmission'
END_FILE_NAME = os.environ['END_FILE_NAME']

def main():
    """
    Processes the info in the datastore into
    """

    datastore_client = datastore.Client(namespace=DS_NAMESPACE)

    storage_client = storage.Client()

    query = datastore_client.query(kind=DS_KIND)
    query_paperform = datastore_client.query(kind=DS_KIND_PAPERFORM)

    excluded = load_excluded_postal_codes()
    keys = load_keys()

    sanitisor = sanitisation.sanitisation.Sanitisor(excluded, keys)
    # todo - potentially shift to writing to disk if / when we move off off app engine
    output = csv.StringIO()
    writer = csv.DictWriter(output, fieldnames=sanitisor.field_names)
    writer.writeheader()
    
    for entity in query.fetch():
        l = sanitisor.sanitise_account(entity)
        for obj in l:
            writer.writerow(obj)

    for entity in query_paperform.fetch():
        l = sanitisor.sanitise_paperform(entity)
        for obj in l:
            writer.writerow(obj)

    curr_time_ms = str(int(time.time() * 1000))
    
    for bucket_name, path in zip(GCS_BUCKETS, GCS_PATHS):
        bucket = storage_client.bucket(bucket_name)
        file_name = os.path.join(path, "-".join([curr_time_ms, END_FILE_NAME]))
        upload_blob(bucket, output.getvalue(), file_name)