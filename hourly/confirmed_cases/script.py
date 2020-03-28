from google.cloud import storage
import pandas as pd
from googleapiclient.discovery import build
import json
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from datetime import datetime
import os
from covidOntario import dispatcher
import pytz

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
# SPREADSHEET_ID = '1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo'
# SPREADSHEET_RANGE = 'Cases'
# GCS_BUCKET = os.environ['GCS_BUCKET']
# UPLOAD_FILE = 'confirmed_data.json'
# SHEETS_API_KEY = os.environ['SHEETS_API_KEY']

# setting environment variables
os.environ['SPREADSHEET_ID'] = "1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo"
os.environ["GCS_BUCKET"] = "flatten-staging-271921.appspot.com"
os.environ['UPLOAD_FILE'] = "confirmed_data.json"
os.environ['SHEETS_API_KEY'] = "AIzaSyDs-bNN44Es1zMpL0pAO4qsnOdz9g4zIok"

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
SPREADSHEET_RANGE = 'Cases'
GCS_BUCKET = os.environ['GCS_BUCKET']
UPLOAD_FILE = os.environ['UPLOAD_FILE']
SHEETS_API_KEY = os.environ['SHEETS_API_KEY']

# Initializes Geolocator and RateLimiter objects
geolocator = Nominatim(user_agent="COVIDScript")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=3)


def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    s = blob.download_as_string()
    return s


def get_spreadsheet_data():
    """
    Gets data on confirmed coronavirus cases from Google's API Client.

    Returns:
        A 2D list representing a list of cases.
    """
    service = build('sheets', 'v4', developerKey=SHEETS_API_KEY)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                      range=SPREADSHEET_RANGE).execute()
    confirmed_cases = result_input.get('values', [])

    # Raise exception if not data is found. Do not remove.
    if not confirmed_cases:
        raise Exception("No data found")

    return confirmed_cases


def preprocess_confirmed_cases_sheet(confirmed_cases):
    """
    Preprocesses the spreadsheet to make it easier to extract Geocode information.

    Parameters:
        confirmed_cases: A Python List of confirmed cases in Canada.

    Returns:
        confirmed_cases_df: A Pandas DataFrame containing the number of confirmed
                            cases per City, Province pair.

        last_updated: The last reported COVID-19 cases and the last time the map
                      was updated.
    """
    df = pd.DataFrame(confirmed_cases)

    # deletes rows where all values are NaN
    df = df.set_index(0)
    df = df.dropna(how="all")
    df.reset_index(inplace=True)

    # sets the first row to the column headers
    df.columns = df.iloc[0]
    df = df[1:]

    # retrieve the date and time that the code runs at
    now = datetime.now(pytz.timezone('US/Eastern'))
    dt_string = now.strftime("%d/%m/%Y %H:%M")
    dt_string.replace('/', '-')

    # compare the time at which the code runs with the last reported COVID-19 Case
    last_updated = "Data last accessed at: " + dt_string + ". Latest case reported on: " + str(
        df.iloc[-1]['date_report']) + "."

    # `health_region` is the city, `province` is the Canadian province
    df = df[['health_region', 'province']]
    df['health_region'] = df['health_region'] + ', ' + df['province']

    # counts the amount of cases per City, Province pair
    confirmed_cases_df = df.groupby('health_region').size()

    return confirmed_cases_df, last_updated


def geocode_sheet(confirmed_cases_df, last_updated):
    """
    Converts text based location information into coordinates the front-end map can use.

    Parameters:
        confirmed_cases_df: A Pandas DataFrame containing the number
                            of confirmed cases per City, Province pair.

    Returns:
        A Python Dictionary containing:
            - The last update time of the map.
            - The greatest amount of cases in a single location.
            - A list of Geocoded coordinates for each location that has reported cases.
    """

    # The keys in this dict are City, Province pairs the Geolocater does not recognize.
    # The script will replace them with the values of the dict.
    name_exceptions = {
        "Kingston Frontenac Lennox & Addington, Ontario": "Kingston, Ontario",
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
        "Zone 4 (Edmundston area), New Brunswick": "Edmundston, New Brunswick",
        "Porcupine, Ontario": "Timmins, Ontario",
        "Central, Alberta": "Red Deer, Alberta",
        "South, Alberta": "Lethbridge, Alberta"
    }

    # the "confirmed_cases" key of this dict will get filled with the Geocoded locations of the confirmed cases
    output = {'last_updated': last_updated, 'max_cases': int(confirmed_cases_df.max()), 'confirmed_cases': []}

    # iterate through the df and geocode data
    for location, count in confirmed_cases_df.iteritems():
        if str(location) == "Not Reported, Repatriated":
            output['confirmed_cases'].append(
                {'name': str(location), 'cases': int(confirmed_cases_df.get(key=str(location))), 'coord': ["N/A", "N/A"]})

        elif str(location)[:12] == "Not Reported":
            if location[14:] == "Ontario":
                continue

            geocoded_location = geocode(location[14:] + ', Canada')
            output['confirmed_cases'].append({'name': str(location), 'cases': int(confirmed_cases_df.get(key=str(location))),
                                              'coord': [geocoded_location.latitude, geocoded_location.longitude]})
            print("Geocoded:" + str(location))

        else:
            if str(location).split(', ')[1] == "Ontario":
                name = str(location)

                try:
                    ## gets scraped ontario data for keys in the spreadsheet
                    cases = dispatcher[name.split(', ')[0]]['func']()['Positive']

                    if cases == 0:
                        continue

                    if location in name_exceptions:
                        geocoded_location = geocode(name_exceptions[str(location)] + ', Canada')

                    else:
                        geocoded_location = geocode(str(location) + ', Canada')

                    output['confirmed_cases'].append(
                        {"name": name, "cases": cases, 'coord': [geocoded_location.latitude, geocoded_location.longitude]})
                    print(f"Geocoded:{str(location)} SCRAPE")
                    dispatcher.pop(name.split(', ')[0], None)
                    continue
                except:
                    pass

            if location in name_exceptions:
                geocoded_location = geocode(
                    name_exceptions[str(location)] + ', Canada')
            else:
                geocoded_location = geocode(str(location) + ', Canada')

            if geocoded_location is None:
                print(location)
                geocoded_location = geocode(str(location).split(", ", 1)[1] + ', Canada')

            output['confirmed_cases'].append({'name': str(location), 'cases': int(confirmed_cases_df.get(key=str(location))),
                                              'coord': [geocoded_location.latitude, geocoded_location.longitude]})
            print("Geocoded:" + str(location))

    ## gets ontario data for keys not in the spreadsheet
    for key in dispatcher.keys():
        try:
            name = key + ", Ontario"
            cases = dispatcher[key]["func"]()['Positive']

            if cases == 0:
                continue

            if name in name_exceptions:
                geocoded_location = geocode(name_exceptions[name] + ', Canada')
            else:
                geocoded_location = geocode(name + ', Canada')
            output['confirmed_cases'].append(
                {"name": name, "cases": cases, 'coord': [geocoded_location.latitude, geocoded_location.longitude]})
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
    output_string = output_string.replace("'", r"\'")

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    upload_blob(bucket, output_string, UPLOAD_FILE)
