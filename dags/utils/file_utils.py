import os
import json
import csv

COMPOSER_DATA_FOLDER = "/home/airflow/gcs/data"

def load_fsa_to_hr(fname="fsa_hr.json"):
    if os.path.exists(COMPOSER_DATA_FOLDER):
        fname = os.path.join(COMPOSER_DATA_FOLDER, fname)
    with open(fname, 'r') as f:
        data = json.load(f)
    return data

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

def csv_map(row, dic, field):
    try:
        res = dic[row[field]]
    except KeyError:
        res = None
    return res