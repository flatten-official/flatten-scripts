from google.cloud import storage
import json


def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket as a string"""
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    s = blob.download_as_string()
    return s


def upload_json(bucket, data_object, dest_file_name):
    """Uploads an object as JSON to a bucket"""
    upload_blob(bucket, json.dumps(data_object).replace("'", r"\'"), dest_file_name)


def upload_blob(bucket, file_string_content, dest_file_name):
    """Uploads a file to the bucket"""
    blob = bucket.blob(dest_file_name)

    blob.upload_from_string(file_string_content)

    print(f"File with content {file_string_content[:110]}.... uploaded to {dest_file_name}.")
