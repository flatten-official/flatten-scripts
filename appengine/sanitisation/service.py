from google.cloud import datastore, storage
import os
import time

GCS_BUCKET = os.environ['GCS_BUCKET']
DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'form-user'
END_FILE_NAME = os.environ['END_FILE_NAME']
FILE_DIRECTORY = os.environ['FILE_DIRECTORY']

# fields for the generated csv
QUESTION_FIELDS = ["q"+str(n) for n in range(1, 8)]
FIELDS = ["postal_code", "timestamp"]+QUESTION_FIELDS


def retrieve_fields(entity):
    """Given an entity from the datastore, generate the list which will be joined to make the fields in the csv."""
    # timestamp is in ms since UNIX origin, so divide by 1000 to get seconds
    fields = [entity['form_responses']['postalCode'].upper(), str(entity['timestamp']/1000.)]
    for field in QUESTION_FIELDS:
        fields.append(entity['form_responses'][field])
    return fields


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

    csv_lines = [",".join(FIELDS)]

    csv_lines.extend(",".join(retrieve_fields(entity)) for entity in query.fetch())

    file_name = os.path.join(FILE_DIRECTORY, "-".join([str(int(time.time())), END_FILE_NAME]))

    upload_blob(bucket, "\n".join(csv_lines), file_name)
