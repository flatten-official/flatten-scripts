import pandas as pd
import datetime as dt
import json
from google.cloud import storage
import os

from gcs.bucket_functions import upload_blob

COUNTRIES = ['Somalia']
UPLOAD_FILE = 'somalia_confirmed.json'
GCS_BUCKET = os.environ['GCS_SAVE_BUCKET']

def get():
    try:
        date = dt.datetime.today().date().strftime("%m-%d-%Y")
        df = pd.read_csv(f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{date}.csv")
    except:
        date = (dt.datetime.today().date() - dt.timedelta(days=1)).strftime("%m-%d-%Y")
        df = pd.read_csv(f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{date}.csv")
    
    rows = []
    for country in COUNTRIES:
        rows.append(df.loc[df['Country_Region'] == country].squeeze().to_dict())
    return rows

def make_geojson():
    data = get()
    FeatureCollection = {"type": "FeatureCollection", "features": []}
    for row in data:
        FeatureCollection["features"].append({"type": "Feature","geometry": { "type": "Point", "coordinates": [row["Long_"], row['Lat']]}, "properties": {"COUNTRY": row['Country_Region'], 'CONFIRMED':int(row['Confirmed']), 'DEATHS': int(row['Deaths']), 'RECOVERED': int(row['Recovered'])}})
    return json.dumps(FeatureCollection)

def main():
    geojson_str = make_geojson()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    upload_blob(bucket, geojson_str, UPLOAD_FILE)