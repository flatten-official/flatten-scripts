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
SPREADSHEET_ID = '1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo'
SPREADSHEET_RANGE = 'Cases'
GCS_BUCKET = os.environ['GCS_BUCKET']
UPLOAD_FILE = 'confirmed_data.json'
SHEETS_API_KEY = os.environ['SHEETS_API_KEY']

# Downloads a blob from the bucket as a string
def download_blob(bucket_name, source_blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    s = blob.download_as_string()
    return s

# Uses the Google Sheets API to access the database of confirmed cases of
# COVID-19 in Canada
# Credit: https://github.com/ishaberry/Covid19Canada
def get_spreadsheet_data():
    service = build('sheets', 'v4', developerKey=SHEETS_API_KEY)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                      range=SPREADSHEET_RANGE).execute()
    values_input = result_input.get('values', [])

    # Part of the sheets API, even if undefined. Do not remove
    if not values_input:
        raise Exception("No data found")

    return values_input

# Uses the spreadsheet and scraper data to assign lat/lon locations
# to each area with a confirmed case
def geocode_sheet(values_input):
    df = pd.DataFrame(values_input)

    # The first 3 rows of the spreadsheet are information regarding the 
    # sheet, so we drop them
    df = df.drop([0, 1, 2])

    column_names = []

    # Assign column names to the dataframe
    for item in df.iloc[0]:
        column_names.append(item)

    df.columns = column_names
    df.index = df.index - 3
    df = df.drop(0)

    print(df.iloc[-1])

    # Provide information on the time of the last update
    now = datetime.now(pytz.timezone('US/Eastern'))
    dt_string = now.strftime("%d/%m/%Y %H:%M")
    dt_string.replace('/', '-')
    last_updated = "Data last accessed at: " + dt_string + ". Latest case reported on: " + str(
        df.iloc[-1]['date_report']) + "."

    # Group the health region and province together for geocoding and grouping by case
    df = df[['health_region', 'province']]
    df['health_region'] = df['health_region'] + ', ' + df['province']
    df = df.groupby('health_region').size()

    # Set up the geocoder, with a rate limiter to deal with timeout issues
    geolocator = Nominatim(user_agent="COVIDScript", timeout=3)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    # Select names that geolocator knows
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
                       "Zone 4 (Edmundston area), New Brunswick": "Edmundston, New Brunswick",
                       "Porcupine, Ontario": "Timmins, Ontario",
                       "Central, Alberta": "Red Deer, Alberta",
                       "South, Alberta": "Lethbridge, Alberta",
                       "Eastern, Ontario": "Cornwall, Ontario",
                       'Central, Saskatchewan': "Humboldt, Saskatchewan"
                       }

    output = {'last_updated': last_updated, 'max_cases': int(df.max()), 'confirmed_cases': []}

    with open("location_cache.json", "r") as json_file:
        location_cache = json.load(json_file)

    # Iterate through all the health regions for geocoding
    for index, row in df.iteritems():
        try:
            # check cache to see if location already exists
            location_info = location_cache[index]
            output['confirmed_cases'].append({
                'name': index,
                'cases': row,
                'coord': location_info["coord"]
            })
        except KeyError:
            # Case for a case with no location information
            if str(index) == "Not Reported, Repatriated":
                output['confirmed_cases'].append(
                    {'name': str(index), 'cases': int(df.get(key=str(index))), 'coord': ["N/A", "N/A"]})

                # add to cache
                location_cache[index] = {
                    "cases": row,
                    "coord": ["N/A", "N/A"]
                }
            # Case for a province only included
            elif str(index)[:12] == "Not Reported":
                # If Ontario, skip as the scraper will deal with that information
                if index[14:] == "Ontario":
                    continue
                location = geocode(index[14:] + ', Canada')
                output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key=str(index))),
                                                'coord': [location.latitude, location.longitude]})
                # add to cache
                location_cache[index] = {
                    "cases": row,
                    "coord": [location.latitude, location.longitude]
                }
                print("Geocoded:" + str(index))
            # Case for all location information provided
            else:
                if str(index).split(', ')[1] == "Ontario":
                    name = str(index)
                    try:
                        ## gets scraped ontario data for keys in the spreadsheet
                        cases = dispatcher[name.split(', ')[0]]['func']()['Positive']

                        if cases == 0:
                            continue

                        if index in name_exceptions:
                            location = geocode(name_exceptions[str(index)] + ', Canada')
                        else:
                            location = geocode(str(index) + ', Canada')
                        output['confirmed_cases'].append(
                            {"name": name, "cases": cases, 'coord': [location.latitude, location.longitude]})

                        location_cache[index] = {
                            "cases": row,
                            "coord": [location.latitude, location.longitude]
                        }

                        print(f"Geocoded:{str(index)} SCRAPE")
                        dispatcher.pop(name.split(', ')[0], None)
                        continue
                    except:
                        pass

                # If the name will not be geocoded, use the value in name_exceptions
                if index in name_exceptions:
                    location = geocode(
                        name_exceptions[str(index)] + ', Canada')
                else:
                    location = geocode(str(index) + ', Canada')

                # Default to province if the location is not found
                if location is None:
                    print(index)
                    location = geocode(str(index).split(", ", 1)[1] + ', Canada')

                output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key=str(index))),
                                                'coord': [location.latitude, location.longitude]})
                # store inside cache
                location_cache[index] = {
                    "cases": row,
                    "coord": [location.latitude, location.longitude]
                }

                print("Geocoded:" + str(index))

    # Gets ontario data for keys not in the spreadsheet
    for key in dispatcher.keys():
        try:
            name = key + ", Ontario"
            cases = dispatcher[key]["func"]()['Positive']

            if cases == 0:
                continue

            if name in name_exceptions:
                location = geocode(name_exceptions[name] + ', Canada')
            else:
                location = geocode(name + ', Canada')
        except:
            print(f"FAILED on {key}")
            continue

        try:
            location_info = location_cache[name]
            output['confirmed_cases'].append({
                'name': name,
                'cases': cases,
                'coord': location_info["coord"]
            })
        except KeyError:
            output['confirmed_cases'].append(
                    {"name": name, "cases": cases, 'coord': [location.latitude, location.longitude]})
            print(f"Geocoded:{name} SCRAPE")

    with open('location_cache.json', 'w') as json_file:
        json.dump(location_cache, json_file, indent=2)

    return output

# Uploads a file to the bucket.
def upload_blob(bucket, data_string, destination_blob_name):

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string)

    print(
        "File {} uploaded to {}.".format(
            data_string, destination_blob_name
        )
    )

# Uploads the JSON to the bucket
def output_json(output):
    output_string = json.dumps(output)
    output_string = output_string.replace("'", r"\'")

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    upload_blob(bucket, output_string, UPLOAD_FILE)
