"""
This scripts
1. Downloads the data from the somalia database in Google datastore
2. Parse the data into 3 arrays of cells
3. Downloads the service account credentials from the secret manager
4. Uploads with that service account the data to the spreadsheet on the google drive
"""

import json
import os
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from utils.secrets import access_secret_version
from utils.bucket_functions import download_blob

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '16cVSGjBRQns1eOrrVmZ-DDRR3LEYGQn-B2J8PXXlBJw'
SHEETS = ['Total Reports', 'Potential', 'Deaths']
BUCKET = 'flatten-staging-dataset'
FILE = 'somalia_data.json'
SECRET_ID = "upload-sheets-somalia"


def main():
    input_data = download_data()
    output_data = parse_data(input_data)
    upload_to_sheets(output_data)


def download_data():
    return json.loads(download_blob(BUCKET, FILE))["region_time_series"]


def parse_data(json_data):
    temp = json_data.pop("all_reports", None)
    if temp:
        data = [[""] + list(temp.keys())]
    else:
        data = []
    # copies the data list into new vars
    potential = data[:]
    deaths = data[:]
    num_reports = data[:]
    for district in json_data.keys():
        row_pot = [district]
        row_death = [district]
        row_reports = [district]
        for date in data[0][1:]:
            try:
                row_pot.append(json_data[district][date]['pot'])
                row_death.append(json_data[district][date]['deaths'])
                row_reports.append(json_data[district][date]['number_reports'])
            except KeyError:
                row_pot.append(0)
                row_death.append(0)
                row_reports.append(0)

        potential.append(row_pot)
        deaths.append(row_death)
        num_reports.append(row_reports)
    return [deaths, num_reports, potential]


def upload_to_sheets(data):
    # Create credentials for Google Sheets API
    project_id = os.environ["GCP_PROJECT"]
    creds_dict = json.loads(access_secret_version(project_id, SECRET_ID, "latest"))
    creds_obj = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)

    service = build('sheets', 'v4', credentials=creds_obj)

    for sheet, value in zip(SHEETS, data):
        service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=sheet, body={'values': value},
                                               valueInputOption='RAW').execute()
    print("Uploaded data to Google Sheets.")


if __name__ == '__main__':
    main()
