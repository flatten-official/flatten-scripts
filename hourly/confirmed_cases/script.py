from google.cloud import storage
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from google.auth.transport.requests import Request
import os
import pickle
import json
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from datetime import datetime

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo'
SAMPLE_RANGE_NAME = 'A1:AA1000'

GCS_BUCKET = 'flatten-271620.appspot.com'
UPLOAD_FILE = 'confirmed_data.json'

def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    s = blob.download_as_string()
    return s

def get_spreadsheet_data():

    api_key = download_blob("flatten_private", "credentials/sheets_api_key.txt")

    service = build('sheets', 'v4', developerKey=api_key)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=SAMPLE_RANGE_NAME).execute()
    values_input = result_input.get('values', [])

    # Part of the sheets API, even if undefined. Do not remove
    if not values_input:
        raise Exception("No data found")

    return values_input


def geocode_sheet(values_input):
    df = pd.DataFrame(values_input)
    df = df.drop([0, 1])

    column_names = []

    for item in df.iloc[0]:
        column_names.append(item)

    df.columns = column_names
    df.index = df.index - 2
    df = df.drop(0)


    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    last_updated = "Data last accessed at: " + dt_string + ". Latest case reported on: " + str(df.iloc[-1]['date_report']) + "."

    df = df[['health_region', 'province']]
    df['health_region'] = df['health_region'] + ', ' + df['province']
    #df.drop('province', axis=1, inplace=True)

    df = df.groupby('health_region').size()
    print(df)

    geolocator = Nominatim(user_agent="COVIDScript")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    name_exceptions = {"Kingston Frontenac Lennox & Addington, Ontario": "Kingston, Ontario",
                       "Zone 2 (Saint John area), New Brunswick": "Saint John, New Brunswick",
                       "Island, BC": "Vancouver Island, BC",
                       "Interior, BC": "Golden, BC",
                       "Grey Bruce, Ontario": "Owen Sound, Ontario"}

    output = {'last_updated': last_updated, 'max_cases': int(df.max()), 'confirmed_cases':[]}

    for index, row in df.iteritems():
        if str(index) == "Not Reported, Repatriated":
            output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key = str(index))), 'coord': ["N/A", "N/A"]})
        elif str(index)[:12] == "Not Reported":
            location = geocode(index[14:] + ', Canada')
            output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key = str(index))), 'coord': [location.latitude, location.longitude]})
            print("Geocoded:" + str(index))
        else:
            if index in name_exceptions:
                location = geocode(
                    name_exceptions[str(index)] + ', Canada')
            else:
                location = geocode(str(index) + ', Canada')

            if location is None:
                location = geocode(str(index).split(", ", 1)[1] + ', Canada')

            output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key = str(index))), 'coord': [location.latitude, location.longitude]})
            print("Geocoded:" + str(index))

    return output


def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            data_string, destination_blob_name
        )
    )


def output_json(output):
    output_string = json.dumps(output)
    output_string = output_string.replace("Vancouver Coastal", "Vancouver")
    output_string = output_string.replace("'", r"\'")
    with open(UPLOAD_FILE, 'w') as outfile:
        outfile.write(output_string)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    upload_blob(bucket, output_string, UPLOAD_FILE)
