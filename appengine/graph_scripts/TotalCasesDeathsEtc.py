import json
import requests
import pandas as pd
import os
from datetime import datetime
from google.cloud import storage

# Set Env Variables
GCS_BUCKET = os.environ['GCS_BUCKET']
UPLOAD_CONFIRMED_TS = "confirmed_time_series.json"

PROVINCES = ["YUKON", "PRINCE EDWARD ISLAND", "NEWFOUNDLAND AND LABRADOR", "NEW BRUNSWICK", "BRITISH COLUMBIA",\
            "NOVA SCOTIA", "SASKATCHEWAN", "ALBERTA", "MANITOBA", "QUEBEC", "ONTARIO", "NORTHWEST TERRITORIES",\
            "NUNAVUT"]

MAPPING = {
    "AB": "ALBERTA",
    "BC": "BRITISH COLUMBIA",
    "SK": "SASKATCHEWAN",
    "MB": "MANITOBA",
    "ON": "ONTARIO",
    "NB": "NEW BRUNSWICK",
    "NL": "NEWFOUNDLAND AND LABRADOR",
    "NS": "NOVA SCOTIA",
    "YT": "YUKON",
    "NT": "NORTHWEST TERRITORIES",
    "PE": "PRINCE EDWARD ISLAND"
}

def convert_unix_timestamp(timestamp):
    """
    Converts unix timestamp (in milliseconds) to a date string

    Parameters:
        timestamp: A long representing milliseconds since Jan. 1st, 1970
    """
    return datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%d')


def upload_blob(TotalCases_data):
    """Uploads a file to the bucket."""

    bucket = storage.Client().bucket(GCS_BUCKET)
    blob = bucket.blob(UPLOAD_CONFIRMED_TS)

    blob.upload_from_string(json.dumps(TotalCases_data))

    print(
        "File {} uploaded to {}.".format(
            TotalCases_data, GCS_BUCKET
        )
    )

def main():
    payload = {
        "f": "pjson",
        "where": "SummaryDate>'1/1/2020 12:00PM'",#change to jan 1 when
        "outFields": "OBJECTID, Province, SummaryDate, TotalCases,TotalTested,TotalDeaths,TotalRecovered"
    }

    # get # of people in ICU per province from Esri
    response = requests.get(
        "https://services9.arcgis.com/pJENMVYPQqZZe20v/arcgis/rest/services/province_daily_totals/FeatureServer/0/query",
        params=payload,
    )

    # extract the part of the response we care about
    esri_TotalCases_data = response.json()["features"]

    # parse esri_TotalCases_data and create the output json file
    TotalCases_data = {}
    for province in PROVINCES:
        TotalCases_data[province] = {"Time Series (Daily)": {}}

    for data in esri_TotalCases_data:
        if data["attributes"]["Province"] == "PEI":
           data["attributes"]["Province"] = "PRINCE EDWARD ISLAND"
        elif data["attributes"]["Province"] == "NWT":
           data["attributes"]["Province"] = "NORTHWEST TERRITORIES"
        elif data["attributes"]["Province"] == "NL":
           data["attributes"]["Province"] = "NEWFOUNDLAND AND LABRADOR"
        elif data["attributes"]["Province"] == "BC":
           data["attributes"]["Province"] = "BRITISH COLUMBIA"
          
        
        attributes = data["attributes"]
        date_str = convert_unix_timestamp(attributes["SummaryDate"])

        if attributes["Province"] not in ["REPATRIATED CDN", "REPATRIATED", "CANADA"]:
            TotalCases_data[attributes["Province"]]["Time Series (Daily)"][date_str] = {"Total Cases": attributes["TotalCases"],"Total Tested": attributes["TotalTested"],"Total Deaths": attributes["TotalDeaths"],"Total Recovered": attributes["TotalRecovered"]}

    # upload to GCS Bucket
    print("Outputting data to file...")
    upload_blob(TotalCases_data)
    print("Done")

if __name__ == "__main__":
    main()
