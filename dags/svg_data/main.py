import pandas as pd
import gcs.bucket_functions as bf
import pandas as pd
from google.cloud import datastore, storage
import json
import os
import datetime as dt

UPLOAD_FILE = 'svg_data.json'

def get_pot_risk(df):
    pot = len(df[(df['probable'] == 'y') & (df['vulnerable'] == 'n')])
    risk = len(df[(df['vulnerable'] == 'y') & (df['probable'] == 'n')])
    both = len(df[(df['vulnerable'] == 'y') & (df['probable'] == 'y')])
    return pot, risk, both

def main():
    from utils import config
    vars = config.load_name_config("svg_data")
    df = bf.get_csv(bucket_name=vars['CSV_BUCKET'],prefix=vars['PREFIX'])
    df.sort_values(by='date', inplace=True)
    # Removes duplicate entries, keeping the most recent
    df.drop_duplicates(subset='id', keep="last", inplace=True)
    svg_data = {"timestamp": int(dt.datetime.now().timestamp()), "postcode": {}}
    # Splits into Canada and US dataframes
    df_can = df[df['country'] == 'ca']
    df_usa = df[df['country'] == 'us']

    for fsa in df_can.fsa.unique():
        df_fsa = df_can[df_can['fsa'] == fsa]
        total = len(df_fsa)
        pot, risk, both = get_pot_risk(df_fsa)
        pot, risk, both = pot/total * 100, risk/total * 100, both/total * 100
        try:
            greatest_need = df_fsa['needs'].dropna().mode()[0]
        except:
            greatest_need = 'na'
        try:
            df_self_iso = df_fsa['self_isolating'].dropna()
            self_iso = df_self_iso.value_counts()['y'] / len(df_self_iso) * 100
        except:
            self_iso = 'na'
        svg_data['postcode'][fsa] = {'pot': pot, 'risk': risk, 'both': both, 'greatest_need': greatest_need, 'self_iso': self_iso}

    for fsa in df_usa.zipcode.unique():
        df_fsa = df_usa[df_usa['zipcode'] == fsa]
        total = len(df_fsa)
        pot, risk, both = get_pot_risk(df_fsa)
        pot, risk, both = pot/total * 100, risk/total * 100, both/total * 100
        try:
            greatest_need = df_fsa['needs'].dropna().mode()[0]
        except:
            greatest_need = 'na'
        try:
            df_self_iso = df_fsa['self_isolating'].dropna()
            self_iso = df_self_iso.value_counts()['y'] / len(df_self_iso) * 100
        except:
            self_iso = 'na'
        svg_data['postcode'][fsa] = {'pot': pot, 'risk': risk, 'both': both, 'greatest_need': greatest_need, 'self_iso': self_iso}
        
    json_str = json.dumps(svg_data)

    storage_client = storage.Client()
    bucket = storage_client.bucket(vars["UPLOAD_BUCKET"])
    bf.upload_blob(bucket, json_str, UPLOAD_FILE)
