from google.cloud import datastore, storage
import pandas as pd


def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            destination_blob_name, bucket
        )
    )


def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    return [blob.name for blob in blobs]


def list_blobs_prefix(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
    return [blob.name for blob in blobs if not blob.name.endswith('README.md')]


def get_csv(bucket_name, prefix):
    csv_path = max(list_blobs_prefix(bucket_name, prefix))
    df = pd.read_csv('gs://' + bucket_name + '/' + csv_path)
    return df


def download_blob(bucket_name, source_blob_name):
    """Downloads a file from the bucket from a string."""

    return storage.Client().bucket(bucket_name).get_blob(source_blob_name).download_as_string()
