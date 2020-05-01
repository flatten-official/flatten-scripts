from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import gcs.bucket_functions as bf
import json
from googleapiclient.discovery import build

SCOPE = ['https://spreadsheets.google.com/feeds']

SPREADSHEET_ID = '16cVSGjBRQns1eOrrVmZ-DDRR3LEYGQn-B2J8PXXlBJw'
SHEETS = ['Total Reports', 'Potential', 'Deaths']
BUCKET = 'flatten-staging-dataset'
FILE = 'somalia_data.json'

def main():
    creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', SCOPE)
    service = build('sheets', 'v4', credentials=creds)

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)

    json_data = json.loads(bf.download_blob(bucket, FILE))["region_time_series"]
    temp = json_data.pop("all_reports",None)
    if temp:
        data = [[""] + list(temp.keys())]
    else:
        data = []
    
    #copies the data list into new vars
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
    
    for sheet, value in zip(SHEETS, [num_reports,potential,deaths]):
        service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=sheet, body={'values': value}, valueInputOption='RAW').execute()
    
if __name__ == '__main__':
    main()
