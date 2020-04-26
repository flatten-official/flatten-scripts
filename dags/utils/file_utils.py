import os
import json
import csv

COMPOSER_DATA_FOLDER = "/home/airflow/gcs/data"

def load_excluded_postal_codes(fname="excluded_postal_codes.csv", join_data_folder=True):
    if join_data_folder:
        fname = os.path.join(COMPOSER_DATA_FOLDER, fname)

    with open(fname) as csvfile:
        reader = csv.reader(csvfile)
        first_row = next(reader)
    return first_row


def load_keys(fname="keys.json", join_data_folder=True):
    if join_data_folder:
        fname = os.path.join(COMPOSER_DATA_FOLDER, fname)

    with open(fname, 'r') as json_file:
        keys = json.load(json_file)
    return keys
