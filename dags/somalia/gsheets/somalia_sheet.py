"""
Documentation of data structure of the data column in an entity of the datastore
        data: {
            "death_age": {          # Matches custom_key
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

import json

from google.cloud import datastore
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from somalia.gsheets.helper import get_column_mapping, get_excluded_column_mapping
from utils.config import load_name_config, get_project_id
from utils.secrets import access_secret_version
from utils.time import get_printable_cur_time

CONFIG = load_name_config('somalia')
SPREADSHEET_ID = CONFIG['sheet_id']
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
TAB_NAMES = ['Data', "Info"]
SECRET_ID = "upload-sheets-somalia"

# Mapping of column to position. e.g. { timestamp: 0, district_name: 2 , ... }
COLUMNS = get_column_mapping()

# Mapping of excluded data to a bool indicating whether the data was ever actually excluded. e.g. { lang: true, ...}
EXCLUDED_COLUMNS = get_excluded_column_mapping()


def main():
    query_iterator = get_reference_to_data()
    data_output = parse_data(query_iterator)
    upload_to_sheets([data_output, get_info_tab()])


def get_reference_to_data():
    """Returns an iterator that allows iterating through the elements of the datastore"""
    client = datastore.Client(namespace=CONFIG["ds_namespace"])
    query = client.query(kind=CONFIG["ds_kind"])
    query.order = ['timestamp']
    return query.fetch()


def parse_data(iterator):
    """Loops through the data and generates the data tab"""

    output = [[""] * len(COLUMNS)]  # Start with a row for the header

    # Create header row
    for column_name, col_index in COLUMNS.items():
        output[0][col_index] = column_name

    timestamp_index = COLUMNS["timestamp"]

    for datastore_row in iterator:
        # Pre populating the array is faster that appending. If no value, defaults to empty string.
        output_row = [""] * len(COLUMNS)

        output_row[timestamp_index] = datastore_row["timestamp"]

        for column_name in datastore_row["data"]:
            if column_name in EXCLUDED_COLUMNS:
                EXCLUDED_COLUMNS[column_name] = True
                continue

            value = str(datastore_row["data"][column_name]["value"])
            if value == "None" or value == "[]":  # Paperform fills in None or [] when the question was never answered
                continue

            try:
                col_index = COLUMNS[column_name]
            except KeyError:
                raise KeyError(column_name + "was a value in the datastore but not in the expected column list. Add "
                                             "it to columns.txt following README.md")

            output_row[col_index] = value

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
        clear_request = service.spreadsheets().values().clear(spreadsheetId=SPREADSHEET_ID, range=tab_name)
        write_request = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=tab_name,
                                                               body={'values': tab_data}, valueInputOption='RAW')

        clear_request.execute()
        write_request.execute()

    print("Uploaded data to Google Sheets.")


def get_info_tab():
    excluded_row = ["The following columns from our form are not shown as they are not useful (e.g. lang (language)"
                    " is always the same)."]

    for col, was_used in EXCLUDED_COLUMNS.items():
        if was_used:
            excluded_row.append(col)
        else:
            print(f"Warning. {col} is an excluded column but was never found in the data.")

    time_row = ["Last updated at: ", get_printable_cur_time()]

    return [excluded_row, time_row]


if __name__ == '__main__':
    main()
