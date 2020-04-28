import gcs.bucket_functions as bf
import pandas as pd
from google.cloud import datastore, storage
import json
import os
from form_data.geo_utils import convert_zip_to_county
import datetime as dt

UPLOAD_FILE = 'form_data.json'
UPLOAD_FILE_USA = 'form_data_usa.json'

def get_pot_risk(df):
    pot = len(df[(df['probable'] == 'y') & (df['vulnerable'] == 'n')])
    risk = len(df[(df['vulnerable'] == 'y') & (df['probable'] == 'n')])
    both = len(df[(df['vulnerable'] == 'y') & (df['probable'] == 'y')])
    return pot, risk, both

def main():
    from utils import config
    vars = config.load_name_config("form_data")
    # Gets the latest sanitised csv
    df = bf.get_csv(bucket_name=vars['CSV_BUCKET'],prefix=vars['PREFIX'])
    # Sorts values from lowest to highest date
    df.sort_values(by='date', inplace=True)
    # Removes duplicate entries, keeping the most recent
    df.drop_duplicates(subset='id', keep="last", inplace=True)
    # Splits into Canada and US dataframes
    df_can = df[df['country'] == 'ca']
    df_usa = df[df['country'] == 'us']

    # Creates dictionaries for usa and canada.
    timestamp = int(dt.datetime.now().timestamp())
    map_data = {'time': timestamp, 'total_responses': 0, 'fsa': {}}
    map_data_usa = {'time': timestamp, 'total_responses': 0, 'zipcode': {}}
    
    for fsa in df_can.fsa.unique():
        df_fsa = df_can[df_can['fsa'] == fsa]
        total = len(df_fsa)
        pot, risk, both = get_pot_risk(df_fsa)
        map_data['total_responses'] += total
        map_data['fsa'][fsa] = {'number_reports': total, 'pot': pot, 'risk': risk, 'both': both}

    for zipcode in df_usa.zipcode.unique():
        df_zip = df_usa[df_usa['zipcode'] == zipcode]
        total = len(df_zip)
        map_data_usa['total_responses'] += total
        pot, risk, both = get_pot_risk(df_zip)
        map_data_usa['zipcode'][int(zipcode)] = {'number_reports': total, 'pot': pot, 'risk': risk, 'both': both}

    map_data_usa = {
        'time': map_data_usa['time'],
        'total_responses': map_data_usa['total_responses'],
        'county': convert_zip_to_county(map_data_usa['zipcode'])
    }
    
    json_str = json.dumps(map_data)
    json_str_usa = json.dumps(map_data_usa)

    storage_client = storage.Client()

    GCS_BUCKETS = vars['GCS_BUCKETS'].split(',')
    GCS_PATHS = vars['GCS_PATHS'][1:][:-1].split(',')
    
    for bucket, path in zip(GCS_BUCKETS, GCS_PATHS):
        bucket = storage_client.bucket(bucket)
        file_path = os.path.join(path, UPLOAD_FILE)
        file_path_usa = os.path.join(path, UPLOAD_FILE_USA)
        bf.upload_blob(bucket, json_str, file_path)
        bf.upload_blob(bucket, json_str_usa, file_path_usa)
