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
import script
import os
import covidOntario
import pytz

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo'
SPREADSHEET_RANGE = 'Cases'
GCS_BUCKET = os.environ['GCS_BUCKET']
UPLOAD_FILE = 'confirmed_data.json'
SHEETS_API_KEY = os.environ['SHEETS_API_KEY']

def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    s = blob.download_as_string()
    return s

def get_spreadsheet_data():

    service = build('sheets', 'v4', developerKey=SHEETS_API_KEY)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                      range=SPREADSHEET_RANGE).execute()
    values_input = result_input.get('values', [])

    # Part of the sheets API, even if undefined. Do not remove
    if not values_input and not values_expansion:
        raise Exception("No data found")

    return values_input


def geocode_sheet(values_input):
    df = pd.DataFrame(values_input)
    df = df.drop([0, 1, 2])

    column_names = []

    for item in df.iloc[0]:
        column_names.append(item)

    df.columns = column_names
    df.index = df.index - 3
    df = df.drop(0)

    print(df.iloc[-1])

    now = datetime.now(pytz.timezone('US/Eastern'))
    dt_string = now.strftime("%d/%m/%Y %H:%M")
    dt_string.replace('/', '-')
    last_updated = "Data last accessed at: " + dt_string + ". Latest case reported on: " + str(df.iloc[-1]['date_report']) + "."

    df = df[['health_region', 'province']]
    df['health_region'] = df['health_region'] + ', ' + df['province']

    df = df.groupby('health_region').size()

    geolocator = Nominatim(user_agent="COVIDScript")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    name_exceptions = {"Kingston Frontenac Lennox & Addington, Ontario": "Kingston, Ontario",
                       "Zone 2 (Saint John area), New Brunswick": "Saint John, New Brunswick",
                       "Island, BC": "Vancouver Island, BC",
                       "Interior, BC": "Golden, BC",
                       "Grey Bruce, Ontario": "Park Head, Ontario",
                       "NWT, NWT": "Northwest Territories",
                       "Haliburton Kawartha Pineridge, Ontario": "Haliburton, Ontario",
                       "Labrador-Grenfell, NL": "Labrador City, NL",
                       "Fraser, BC": "Fraser Valley, BC",
                       "Zone 3 (Fredericton area), New Brunswick": "Fredericton, New Brunswick",
                       "Zone 1 (Moncton Area), New Brunswick": "Moncton, New Brunswick",
                       "North, Saskatchewan": "La Ronge, Saskatchewan",
                       "North, Alberta": "Peerless Lake, Alberta",
                       "South, Saskatchewan": "Moose Jaw, Saskatchewan",
                       "North Bay Parry Sound, Ontario": "North Bay, Ontario",
                       "Leeds Grenville Lanark": "Brockville, Ontario",
                       "Southwestern": "St. Thomas, Ontario",
                       "Zone 4 (Edmundston area), New Brunswick": "Edmundston, New Brunswick"
                       }

    output = {'last_updated': last_updated, 'max_cases': int(df.max()), 'confirmed_cases':[]}

    for index, row in df.iteritems():
        if str(index) == "Not Reported, Repatriated":
            output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key = str(index))), 'coord': ["N/A", "N/A"]})
        elif str(index)[:12] == "Not Reported":
            if index[14:] == "Ontario":
                continue
            location = geocode(index[14:] + ', Canada')
            output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key = str(index))), 'coord': [location.latitude, location.longitude]})
            print("Geocoded:" + str(index))
        else:
            if str(index).split(', ')[1] == "Ontario":
                name = str(index)
                try:
                    cases = dispatcher[name.split(', ')[0]]()['Positive']
                    if index in name_exceptions:
                        location = geocode(name_exceptions[str(index)] + ', Canada')
                    else:
                        location = geocode(str(index) + ', Canada')
                    output['confirmed_cases'].append({"name": name, "cases": cases, 'coord': [location.latitude, location.longitude]})
                    print(f"Geocoded:{str(index)} SCRAPE")
                    dispatcher.pop(name.split(', ')[0], None)
                    continue
                except:
                    pass

            if index in name_exceptions:
                location = geocode(
                    name_exceptions[str(index)] + ', Canada')
            else:
                location = geocode(str(index) + ', Canada')

            if location is None:
                print(index)
                location = geocode(str(index).split(", ", 1)[1] + ', Canada')

            output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key = str(index))), 'coord': [location.latitude, location.longitude]})
            print("Geocoded:" + str(index))

    for key in dispatcher.keys():
        try:
            name = key+", Ontario"
            cases = dispatcher[key]()
            if name in name_exceptions:
                location = geocode(name_exceptions[name] + ', Canada')
            else:
                location = geocode(name + ', Canada')
            output['confirmed_cases'].append({"name": name, "cases": cases, 'coords': [location.latitude, location.longitude]})
            print(f"Geocoded:{name} SCRAPE")
        except:
            print(f"FAILED on {key}")

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

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    upload_blob(bucket, output_string, UPLOAD_FILE)

dispatcher = {
    'Algoma': covidOntario.get_algoma_data,
    'Brant': covidOntario.getBrantCountyData,
    'Chatham-Kent': covidOntario.getChathamKentData,
    'Durham': covidOntario.getDurhamData,
    'Eastern': covidOntario.getEasternOntarioData,
    #Grey Bruce to go here when ready
    #Halimand Norfolk to go here when ready
    "Haliburton Kawartha Pineridge": covidOntario.getHaliburtonKawarthaPineRidgeData,
    "Halton": covidOntario.getHaltonData,
    "Hamilton": covidOntario.getHamiltonData,
    "Hastings Prince Edward": covidOntario.getHastingsPrinceEdwardData,
    "Huron Perth": covidOntario.getHuronData,
    "Kingston Frontenac Lennox & Addington": covidOntario.getKingstonFrontenacLennoxAddingtonData,
    "Lambton County": covidOntario.getLambtonData,
    "Leeds Grenville Lanark": covidOntario.getLeedsGrenvilleLanarkData,
    "Middlesex-London": covidOntario.getMiddlesexLondonData,
    "Niagara": covidOntario.getNiagaraData,
    "North Bay Parry Sound": covidOntario.getNorthBayParrySoundData,
    "Northwestern": covidOntario.getNorthWesternData,
    "Ottawa": covidOntario.getOttawaData,
    "Peel": covidOntario.getPeelData,
    "Peterborough": covidOntario.getPeterboroughData,
    "Porcupine": covidOntario.getPorcupineData,
    "Renfrew": covidOntario.getRenfrewCountyData,
    "Simcoe Muskoka": covidOntario.getSimcoeMuskokaData,
    "Southwestern": covidOntario.getSouthwesternData,
    "Sudbury": covidOntario.getSudburyData,
    "Thunder Bay": covidOntario.getThunderBayData,
    "Timiskaming": covidOntario.getTimiskamingData,
    "Toronto": covidOntario.getTorontoData,
    "Waterloo": covidOntario.getWaterlooData,
    "Wellington Dufferin Guelph": covidOntario.getWellingtonDufferinGuelphData,
    "Windsor-Essex": covidOntario.getWindsorEssexCountyData,
    "York": covidOntario.getYorkData
}