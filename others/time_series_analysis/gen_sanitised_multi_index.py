import pandas as pd
import numpy as np
from google.cloud import storage

def list_blobs_prefix(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
    return [blob.name for blob in blobs if not blob.name.endswith('README.md')]


def get_csv(bucket_name, prefix):
    csv_path = max(list_blobs_prefix(bucket_name, prefix))
    df = pd.read_csv('gs://' + bucket_name + '/' + csv_path)
    return df


def gen_sanitised_by_date_fsa():
    df = get_csv('flatten-dataset', 'flatten-form-data-v1')
    df=df[df['country']=='ca'][['date', 'fsa', 'probable']]
    df['probable'] = df['probable'].map(lambda x: int(x=='y'))
    df = df.groupby(['date', 'fsa']).agg({'probable': ['sum', 'count']})
    df = df.reset_index()

    idx = pd.date_range(min(df['date']), max(df['date']))
    by_fsa = {}
    for fsa in df['fsa'].unique():
        by_fsa[fsa] = df[df['fsa']==fsa].groupby('date').mean().reset_index().set_index('date')
        by_fsa[fsa].index = pd.DatetimeIndex(by_fsa[fsa].index)
        by_fsa[fsa] = by_fsa[fsa].reindex(idx, fill_value=0)
        by_fsa[fsa]['fsa'] = fsa
    return pd.concat(fsa_data for fsa_data in by_fsa.values())

if __name__ == "__main__":
    df = gen_sanitised_by_date_fsa()
    df.to_csv('flatten_probable_by_date_fsa.csv', index=True)