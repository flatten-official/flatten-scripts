from google.cloud import datastore, storage
import datetime
import os

GCS_BUCKETS = os.environ['GCS_BUCKETS'].split(',')
GCS_PATHS = os.environ['GCS_PATHS'].split(',')
END_FILE_NAME = 'form_data.json'
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'FlattenMarketing'
HEADER = 'email,latest'


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

    csv_lines = [HEADER]

    emails = {}

    for entity in query.fetch():
        email = entity['email'].lower()
        if email in emails:
            emails[email]['latest'] = max(emails[email]['latest'], entity['latest'])
        else:
            emails[email] = {
                'email': email,
                'latest': entity['latest']
            }
    csv_lines.extend(
        ','.join([o['email'], str(o['latest'])]) for o in emails.values()
    )

    csv_string = "\n".join(csv_lines)

    curr_time_ms = str(int(datetime.datetime.utcnow().strftime("%s"))*1000)

    for bucket_name, path in zip(GCS_BUCKETS, GCS_PATHS):
        bucket = storage_client.bucket(bucket_name)
        file_name = os.path.join(path, "-".join([curr_time_ms, END_FILE_NAME]))
        upload_blob(bucket, csv_string, file_name)
