import os
import json
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage

GCS_BUCKET = os.environ.get('GCS_SAVE_BUCKET')
UPLOAD_ICU = "ICU_capacity.json"

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

def insert_max_ICU_capacity(ICU_data):
    # data = pd.read_csv("ICU-capacity.csv")

    data = pd.read_csv("gcs/data/ICU-capacity.csv")

    # isolate the relevant columns
    province_to_ICU_bed_mapping = data[["Province", "Intensive Care"]]
    province_to_ICU_bed_mapping = province_to_ICU_bed_mapping.replace(" — ", "0")

    # reformat the nums to remove whitespace and turn them from str to int
    unprocessed_nums = list(province_to_ICU_bed_mapping["Intensive Care"])
    processed_list = []
    for nums in unprocessed_nums:
        processed_list.append(int(nums.strip()))
    processed_list

    # append the newly processed row back into the df
    province_to_ICU_bed_mapping["Intensive Care"] = processed_list

    # aggregate the ICU capacity by province
    province_to_ICU_bed_mapping = province_to_ICU_bed_mapping.groupby("Province", as_index=False).sum()

    # replace Province abbreviations with actual names
    province_to_ICU_bed_mapping = province_to_ICU_bed_mapping.replace({"Province": MAPPING})

    # place max icu capacity into correct dict locations
    for index, fields in province_to_ICU_bed_mapping.iterrows():
        ICU_data[fields["Province"]]["Max Capacity"] = fields["Intensive Care"]

    # data is missing for Quebec and Nunavut
    ICU_data["NUNAVUT"]["Max Capacity"] = None
    ICU_data["QUEBEC"]["Max Capacity"] = None

    return ICU_data

def upload_blob(source_file):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    bucket = storage.Client().bucket(GCS_BUCKET)
    blob = bucket.blob(UPLOAD_ICU)

    blob.upload_from_string(source_file)

    print(
        "File {} uploaded to {}.".format(
            UPLOAD_ICU, GCS_BUCKET
        )
    )

def main():
    payload = {
        "f": "pjson",
        "where": "SummaryDate>'4/8/2020 12:00PM'",
        "outFields": "OBJECTID, Province, SummaryDate, TotalICU"
    }

    # get # of people in ICU per province from Esri
    response = requests.get(
        "https://services9.arcgis.com/pJENMVYPQqZZe20v/arcgis/rest/services/province_daily_totals/FeatureServer/0/query",
        params=payload,
    )

    # extract the part of the response we care about
    esri_ICU_data = response.json()["features"]

    # parse esri_ICU_data and create the output json file
    ICU_data = {}
    for province in PROVINCES:
        ICU_data[province] = {"Time Series (Daily)": {}}

    for data in esri_ICU_data:
        attributes = data["attributes"]
        date_str = convert_unix_timestamp(attributes["SummaryDate"])

        if attributes["Province"] not in ["REPATRIATED CDN", "REPATRIATED", "CANADA"]:
            ICU_data[attributes["Province"]]["Time Series (Daily)"][date_str] = {"Total ICU": attributes["TotalICU"]}

    # insert max_capacity into the return dict
    ICU_data = insert_max_ICU_capacity(ICU_data)

    print(ICU_data)

    # with open("icu_capacity.json", "w") as ICU_capacity_json:
    #     json.dump(ICU_data, ICU_capacity_json, indent=2)

    upload_blob(json.dumps(ICU_data))

if __name__ == "__main__":
    main()
