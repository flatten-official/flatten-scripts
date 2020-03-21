from google.cloud import storage
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from google.auth.transport.requests import Request
import os
import pickle
import json
from geopy.geocoders import Nominatim

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo'
SAMPLE_RANGE_NAME = 'A1:AA1000'

GCS_BUCKET = 'flatten-271620.appspot.com'
UPLOAD_FILE = 'confirmed_data.js'


def get_spreadsheet_data():
    creds = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json',
                                                             SCOPES)  # here enter the name of your downloaded JSON file
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=SAMPLE_RANGE_NAME).execute()
    values_input = result_input.get('values', [])

    # Part of the sheets API, even if undefined. Do not remove
    if not values_input and not values_expansion:
        raise Exception("No data found")

    return values_input


def geocode_sheet(values_input):
    df = pd.DataFrame(values_input)

    last_updated = df[0][0]

    df = df.drop([0, 1])

    column_names = []

    for item in df.iloc[0]:
        column_names.append(item)

    df.columns = column_names
    df.index = df.index - 2
    df = df.drop(0)

    geolocator = Nominatim(user_agent="COVIDScript")

    name_exceptions = {"Kingston Frontenac Lennox & Addington": "Kingston",
                       "Zone 2 (Saint John area)": "Saint John",
                       "Island": "Vancouver Island",
                       "Interior": "Golden"}

    output = {}

    for index, row in df.iterrows():
        if row['health_region'] + ', ' + row['province'] in output:
            output[row['health_region'] + ', ' + row['province']][0] += 1
        else:
            if row['health_region'] == "Not Reported" and row['province'] == "Repatriated":
                output[row['health_region'] + ', ' + row['province']] = [1, "N/A", "N/A"]
            elif row['health_region'] == "Not Reported":
                location = geolocator.geocode(row['province'] + ', Canada')
                output[row['health_region'] + ', ' + row['province']] = [1, location.latitude, location.longitude]
            else:
                if row['health_region'] in name_exceptions:
                    location = geolocator.geocode(
                        name_exceptions[row['health_region']] + ', ' + row['province'] + ', Canada')
                else:
                    location = geolocator.geocode(row['health_region'] + ', ' + row['province'] + ', Canada')

                if location is None:
                    location = geolocator.geocode(row['province'] + ', Canada')
                output[row['health_region'] + ', ' + row['province']] = [1, location.latitude, location.longitude]

    return output, last_updated


def upload_blob(bucket, data_string, destination_blob_name):
    """Uploads a file to the bucket."""

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            data_string, destination_blob_name
        )
    )


def output_json(output, last_updated):
    real_output = []

    for key in output:
        real_output.append({"name": key, "cases": output[key][0], "coord": [output[key][1], output[key][2]]})

    output_string = json.dumps(real_output)
    output_string = output_string.replace("Vancouver Coastal", "Vancouver")
    output_string = output_string.replace("'", r"\'")
    output_string = "data_last_updated = '" + last_updated + "';\n" + "data_confirmed = '" + output_string + "';"
    with open(UPLOAD_FILE, 'w') as outfile:
        outfile.write(output_string)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    upload_blob(bucket, output_string, UPLOAD_FILE)
