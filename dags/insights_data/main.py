import pandas as pd
import gcs.bucket_functions as bf
from google.cloud import datastore, storage
import json
import datetime as dt

def get_pot_risk(df):
    pot = len(df[(df['probable'] == 'y') & (df['vulnerable'] == 'n')])
    risk = len(df[(df['vulnerable'] == 'y') & (df['probable'] == 'n')])
    both = len(df[(df['vulnerable'] == 'y') & (df['probable'] == 'y')])
    return pot, risk, both

def main():
    from utils import config
    vars = config.load_name_config("insights_data")
    df = bf.get_csv(bucket_name=vars['CSV_BUCKET'],prefix=vars['PREFIX'])
    df.sort_values(by='date', inplace=True)
    # Removes duplicate entries, keeping the most recent
    df.drop_duplicates(subset='id', keep="last", inplace=True)
    data = {"timestamp": int(dt.datetime.now().timestamp()), "postcode": {}}
    # Splits into Canada and US dataframes
    df_can = df[df['country'] == 'ca']
    df_usa = df[df['country'] == 'us']

    for fsa in df_can.fsa.unique():
        df_fsa = df_can[df_can['fsa'] == fsa]
        count = len(df_fsa)
        pot, risk, both = get_pot_risk(df_fsa)

        try:
            needs = df_fsa['needs'].dropna()
            greatest_need = needs.mode()[0]
            greatest_need_count = len(needs)
        except:
            greatest_need = None
            greatest_need_count = 0

        try:
            df_self_iso = df_fsa['self_isolating'].dropna()
            self_iso = int(df_self_iso.value_counts()['y'])
            self_iso_count = len(df_self_iso)
        except:
            self_iso = None
            self_iso_count = 0

        data['postcode'][fsa] = {
            'basic_total': count,
            'pot': pot,
            'risk': risk,
            'both': both,
            'greatest_need': greatest_need,
            'greatest_need_total': greatest_need_count,
            'self_iso': self_iso,
            'self_iso_total': self_iso_count
        }

    for fsa in df_usa.zipcode.unique():
        df_fsa = df_usa[df_usa['zipcode'] == fsa]
        count = len(df_fsa)
        pot, risk, both = get_pot_risk(df_fsa)

        try:
            needs = df_fsa['needs'].dropna()
            greatest_need = needs.mode()[0]
            greatest_need_count = len(needs)
        except:
            greatest_need = None
            greatest_need_count = 0

        try:
            df_self_iso = df_fsa['self_isolating'].dropna()
            self_iso = int(df_self_iso.value_counts()['y'])
            self_iso_count = len(df_self_iso)
        except:
            self_iso = None
            self_iso_count = 0

        data['postcode'][fsa] = {
            'basic_total': count,
            'pot': pot,
            'risk': risk,
            'both': both,
            'greatest_need': greatest_need,
            'greatest_need_total': greatest_need_count,
            'self_iso': self_iso,
            'self_iso_total': self_iso_count
        }
        
    json_str = json.dumps(data)

    storage_client = storage.Client()
    bucket = storage_client.bucket(vars["UPLOAD_BUCKET"])
    bf.upload_blob(bucket, json_str, vars["UPLOAD_FILE"])
