import json
import os
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.auth import compute_engine

from utils.secrets import access_secret_version
from gcs.bucket_functions import download_blob

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '16cVSGjBRQns1eOrrVmZ-DDRR3LEYGQn-B2J8PXXlBJw'
SHEETS = ['Total Reports', 'Potential', 'Deaths']
BUCKET = 'flatten-staging-dataset'
FILE = 'somalia_data.json'
SECRET_ID = "upload-sheets-somalia"


def main():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)

    json_data = json.loads(download_blob(bucket, FILE))["region_time_series"]
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

    upload_to_sheets([num_reports, potential, deaths])


def upload_to_sheets(data):
    # Create credentials for Google Sheets API
    project_id = os.environ["GCP_PROJECT"]
    creds_str = access_secret_version(project_id, SECRET_ID, "latest")
    creds_dict = json.loads(creds_str)
    creds_obj = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)

    service = build('sheets', 'v4', credentials=creds_obj)

    for sheet, value in zip(SHEETS, data):
        service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=sheet, body={'values': value},
                                               valueInputOption='RAW').execute()


if __name__ == '__main__':
    main()
