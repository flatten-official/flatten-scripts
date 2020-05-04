"""
This scripts
1. Downloads the data from the somalia database in Google datastore
2. Parse the data into 3 arrays of cells
3. Downloads the service account credentials from the secret manager
4. Uploads with that service account the data to the spreadsheet on the google drive
"""

import json
import os

from google.cloud import datastore
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from utils.config import load_name_config, get_project_id
from utils.secrets import access_secret_version
from utils.gcp_helpers import download_blob

config = load_name_config('somalia')

SPREADSHEET_ID = config['sheet_id']
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
TAB_NAMES = ['Data', "Excluded Data"]
SECRET_ID = "upload-sheets-somalia"
COLUMN_FILE = "./somalia/gsheets/columns.txt"
EXCLUDED_COLUMN_FILE = "./somalia/gsheets/excluded_columns.txt"

"""
# Data structure of the data column in an entity of the datastore
data: {
    "death_age": {
      "entityValue": {
        "properties": {
          "type": {
            "stringValue": "number"
          },
          "value": {
            "nullValue": null
          },
          "custom_key": {
            "stringValue": "death_age"
          },
          "title": {
            "stringValue": "Da’da qofka dhintay"
          },
          "key": {
            "stringValue": "6aqpk"
          },
          "description": {
            "nullValue": null
          }
        }
      }
    },
    .
    .
    .
  }
"""


def upload_somalia_data_to_sheets():
    query_iterator = get_data_reference()
    excluded_columns, excluded_tab = get_excluded_columns_from_file()
    data = parse_data(query_iterator, excluded_columns)
    upload_to_sheets([data, excluded_tab])


def get_data_reference():
    datastore_client = datastore.Client(namespace=config["ds_namespace"])
    query = datastore_client.query(kind=config["ds_kind"])
    query.order = ['timestamp']
    return query.fetch()


def parse_data(iterator, excluded_columns):
    expected_columns, column_map = get_columns_from_file()  # Column map maps a column to a position (index) in the row

    output = [expected_columns]  # Start with the columns as the header

    timestamp_index = column_map["timestamp"]

    for datastore_row in iterator:
        # Pre populating the array is faster that appending. If no value, defaults to empty string.
        output_row = [""] * len(expected_columns)

        output_row[timestamp_index] = datastore_row["timestamp"]

        for column_name in datastore_row["data"]:
            if column_name in excluded_columns:
                continue

            try:
                index = column_map[column_name]
            except KeyError:
                raise KeyError("    " +
                               column_name + "    was a value in the datastore but not in the expected column list. Add it to "
                                             "columns.txt following README.md")

            output_row[index] = str(datastore_row["data"][column_name]["value"])

        output.append(output_row)

    return output


def upload_to_sheets(data):
    """
    Upload a list of rows to the spreadsheet using the credentials in the GCP Secret Manager
    Useful docs :
    https://developers.google.com/sheets/api/quickstart/python
    https://developers.google.com/sheets/api/guides/values#python_2
    """

    # Get credentials for Google Sheets API
    secret_payload = access_secret_version(get_project_id(), SECRET_ID, "latest")
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(secret_payload), SCOPES)

    # Upload to Google Sheets through the Google Sheets API
    service = build('sheets', 'v4', credentials=credentials)

    for tab_name, tab_data in zip(TAB_NAMES, data):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=tab_name,
            body={'values': tab_data},
            valueInputOption='RAW'
        ).execute()

    print("Uploaded data to Google Sheets.")


def get_columns_from_file():
    """
    Reads the column names from columns.txt
    Returns a list of column names as well as a mapping of the position of these columns
    """
    with open(COLUMN_FILE, "r") as f:
        columns = f.read().rstrip().split("\n")
        # Mapping of column to column index
        column_map = {}

        # Populate mapping
        for i in range(len(columns)):
            column_map[columns[i]] = i

    return columns, column_map


def get_excluded_columns_from_file():
    """
    Returns a list of excluded columns as well as a tab indicating excluded columns.
    """
    with open(EXCLUDED_COLUMN_FILE, "r") as f:
        exc_columns = f.read().rstrip().split("\n")

    tab = [["The following columns from our data our not shown as they are not useful."]]  # Nested since it's 2D array

    for col in exc_columns:
        tab.append([col])

    return exc_columns, tab


if __name__ == '__main__':
    upload_somalia_data_to_sheets()
