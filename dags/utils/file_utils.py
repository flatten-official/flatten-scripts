import os
import json
import csv

COMPOSER_DATA_FOLDER = "/home/airflow/gcs/data"

def load_excluded_postal_codes(fname="excluded_postal_codes.csv"):

    if os.path.exists(COMPOSER_DATA_FOLDER):
        fname = os.path.join(COMPOSER_DATA_FOLDER, fname)

    with open(fname) as csvfile:
        reader = csv.reader(csvfile)
        first_row = next(reader)
    return first_row


def load_keys(fname="keys.json"):

    if os.path.exists(COMPOSER_DATA_FOLDER):
        fname = os.path.join(COMPOSER_DATA_FOLDER, fname)

    with open(fname, 'r') as json_file:
        keys = json.load(json_file)
    return keys
