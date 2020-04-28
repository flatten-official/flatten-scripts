from google.cloud import storage
from gcs.bucket_functions import upload_blob
import json

def upload_json(bucket, data_object, dest_file_name):
    """Uploads an object as JSON to a bucket"""
    upload_blob(bucket, json.dumps(data_object).replace("'", r"\'"), dest_file_name)


def upload_blob(bucket, file_string_content, dest_file_name):
    """Uploads a file to the bucket"""
    blob = bucket.blob(dest_file_name)

    blob.upload_from_string(file_string_content)

    print(f"File with content {file_string_content[:110]}.... uploaded to {dest_file_name}.")


def write_json_to_disk(data, filepath):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
