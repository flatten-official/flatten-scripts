"""
Read README.md for documentation of code.
"""

import json

from google.cloud import datastore
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from somalia.gsheets.helper import get_column_mapping, get_excluded_column_mapping
from utils.config import load_name_config, get_project_id
from utils.secrets import access_secret_version
from utils.time import get_printable_cur_time

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
TAB_NAMES = ['Data', "Info"]

CONFIG = load_name_config('somalia')

# Mapping of column to position. e.g. { timestamp: 0, district_name: 2 , ... }
COLUMNS = get_column_mapping()

# Mapping of excluded data to a bool indicating whether the data was ever actually excluded. e.g. { lang: true, ...}
EXCLUDED_COLUMNS = get_excluded_column_mapping()


def main():
    data_iterator = get_data()

    data_output = parse_data(data_iterator)

    format_data(data_output)

    upload_data([data_output, get_info_tab()])


def get_data():
    """Returns an iterator that allows iterating through the elements of the datastore"""
    client = datastore.Client(namespace=CONFIG["ds_namespace"])
    query = client.query(kind=CONFIG["ds_kind"])
    query.order = ['timestamp']
    return query.fetch()


def parse_data(iterator):
    """Loops through the data and generates the data tab"""
    # Create header row
    header_row = []
    for column_name, col_index in COLUMNS.items():
        header_row.append(column_name)

    output = [header_row]

    timestamp_index = COLUMNS["timestamp"]

    for datastore_row in iterator:
        # Pre populating the array is faster that appending. If no value, defaults to empty string.
        output_row = [""] * len(COLUMNS)

        output_row[timestamp_index] = datastore_row["timestamp"]

        for column_name in datastore_row["data"]:
            if column_name in EXCLUDED_COLUMNS:
                EXCLUDED_COLUMNS[column_name] = True
                continue

            try:
                col_index = COLUMNS[column_name]
            except KeyError:
                raise KeyError(column_name + "was a value in the datastore but not in the expected column list. Add "
                                             "it to columns.txt following README.md")

            output_row[col_index] = str(datastore_row["data"][column_name]["value"])

        output.append(output_row)

    return output


def format_data(data):
    """Formats the paperform data to be more friendly."""
    for i, row in enumerate(data):
        for j, cell in enumerate(row):
            # Paperform fills in None or [] when the question was never answered
            if cell == "[]" or cell == "None":
                data[i][j] = ""


def upload_data(data):
    """
    Upload a list of rows to the spreadsheet using the credentials in the GCP Secret Manager
    Useful docs :
    https://developers.google.com/sheets/api/quickstart/python
    https://developers.google.com/sheets/api/guides/values#python_2
    """

    # Get credentials for Google Sheets API
    secret_payload = access_secret_version(get_project_id(), CONFIG['sheets_service_account_secret_id'], "latest")
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(secret_payload), SCOPES)
    spreadsheet_id = CONFIG['sheet_id']

    # Upload to Google Sheets through the Google Sheets API
    service = build('sheets', 'v4', credentials=credentials)

    for tab_name, tab_data in zip(TAB_NAMES, data):
        clear_request = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=tab_name)
        write_request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=tab_name,
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
