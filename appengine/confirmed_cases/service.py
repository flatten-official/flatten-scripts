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
SPREADSHEET_CASES = 'Cases'
SPREADSHEET_REC = 'Recovered'
SPREADSHEET_DEATH = 'Mortality'
GCS_BUCKET = os.environ['GCS_BUCKET']
UPLOAD_CONFIRMED = 'confirmed_data.json'
UPLOAD_TRAVEL = 'travel_data.json'
UPLOAD_PROVINCIAL = 'provincial_data.json'
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
                                      range=SPREADSHEET_CASES).execute()
    values_input = result_input.get('values', [])

    result2_input = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                      range=SPREADSHEET_REC).execute()
    values2_input = result2_input.get('values', [])     

    result3_input = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                      range=SPREADSHEET_DEATH).execute()
    values3_input = result3_input.get('values', [])   

    # Part of the sheets API, even if undefined. Do not remove
    if not (values_input and values2_input and values3_input):
        raise Exception("No data found")

    return values_input, values2_input, values3_input

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
    geolocator = Nominatim(user_agent="COVIDScript")
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
                       'Central, Saskatchewan': "Humboldt, Saskatchewan",
                       'Algoma, Ontario': 'Sault Ste. Marie, Ontario',
                       'Wellington Dufferin Guelph, Ontario': "Guelph, Ontario"
                       }

    output = {'last_updated': last_updated, 'max_cases': int(df.max()), 'confirmed_cases': []}

    # Iterate through all the health regions for geocoding
    for index, row in df.iteritems():
        # Case for a case with no location information
        if str(index) == "Not Reported, Repatriated":
            output['confirmed_cases'].append(
                {'name': str(index), 'cases': int(df.get(key=str(index))), 'coord': ["N/A", "N/A"]})
        # Case for a province only included
        elif str(index)[:12] == "Not Reported":
            # If Ontario, skip as the scraper will deal with that information
            if index[14:] == "Ontario":
                continue
            location = geocode(index[14:] + ', Canada')
            output['confirmed_cases'].append({'name': str(index), 'cases': int(df.get(key=str(index))),
                                              'coord': [location.latitude, location.longitude]})
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
            output['confirmed_cases'].append(
                {"name": name, "cases": cases, 'coord': [location.latitude, location.longitude]})
            print(f"Geocoded:{name} SCRAPE")
        except:
            print(f"FAILED on {key}")

    return output

def reformat_df(spreadsheet_data):
    ## Loads data

    df = pd.DataFrame(spreadsheet_data)

    ## Reformats for useful column names

    df = df.drop([0, 1, 2])
    column_names = []
    for item in df.iloc[0]:
        column_names.append(item)
    df.columns = column_names
    df.index = df.index - 3
    df = df.drop(0)
    return df

def get_travel_data(spreadsheet_data):
    ## Loads data

    df = reformat_df(spreadsheet_data)

    # Collects data

    has_travelled = df.loc[df['travel_yn'] == '1']['travel_history_country']
    not_travelled = df.loc[df['travel_yn'] == '0']['locally_acquired']
    # Counts each instance of a travel location / method of infection and stores in a dict
    travel_data = has_travelled.value_counts().to_dict()
    no_travel_data = not_travelled.value_counts().to_dict()

    ## no_travel_data contains both 'Close contact' and 'Close Contact' this is to merge them
    no_travel_data['Close Contact'] += no_travel_data['Close contact']
    no_travel_data.pop('Close contact', None)

    total_cases = len(df)
    travel_cases = len(has_travelled)
    no_travel_cases = len(not_travelled)

    return { "cases": total_cases, 
             "travel_cases": travel_cases, 
             "no_travel_cases": no_travel_cases, 
             "not_reported": total_cases - travel_cases - no_travel_cases,
             "travel_data": travel_data,
             "no_travel_data": no_travel_data }

def get_provincial_totals(output_dict, rec_spreadsheet, dead_spreadsheet):

    ## Loads new spreadsheets

    rec_df = reformat_df(rec_spreadsheet)
    dead_df = reformat_df(dead_spreadsheet)

    #creates provincial data dictionary
    provincial_data = {}

    # adds case numbers to their respective province/territory
    for region in output_dict['confirmed_cases']:
        province = region["name"].split(", ")[1]

        # if province already has a subdictionary augment the cases value
        if province in provincial_data:
            provincial_data[province]['cases'] += region['cases']
        #otherwise create the subdictionary and initialize cases value to the regional case count
        else:
            provincial_data[province] = {'cases': region['cases']}

    # Adds recovery and mortality data to each province
    for province in provincial_data.keys():
        # Skip repatriated as there is no data
        if province == 'Repatriated':
            continue

        ## This Block Deals with recovery data -----------------------------------------------------
        # selects only the columns we are interested in
        rec_data = rec_df.loc[rec_df['province'] == province].loc[:, ['date_recovered', 'cumulative_recovered']]
        ## in a try except since some provinces have no deaths
        #drops not available data
        rec_data = rec_data[rec_data.cumulative_recovered != "NA"]
        # converts counts to integer
        rec_data['cumulative_recovered'] = rec_data['cumulative_recovered'].astype(int)
        # converts to dictionary format with date as key and cumulative count as value
        rec_data = rec_data.set_index('date_recovered')['cumulative_recovered'].to_dict()
        provincial_data[province]['recovered_cumul'] = rec_data

        ## This Block Deals with Death data ---------------------------------------------------------
        # selects the column containing date of reported death and counts the number of deaths on that day
        # also converts to dictionary form
        death_data = dead_df.loc[dead_df['province'] == province]['date_death_report'].value_counts(sort=False).to_dict()
        provincial_data[province]['dead_daily'] = death_data

    return provincial_data

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
def output_json(output, travel_out, provincial_out):
    output_string = json.dumps(output).replace("'", r"\'")

    travel_string = json.dumps(travel_out).replace("'", r"\'")
    provincial_string = json.dumps(provincial_out).replace("'", r"\'")

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    upload_blob(bucket, output_string, UPLOAD_CONFIRMED)
    upload_blob(bucket, travel_string, UPLOAD_TRAVEL)
    upload_blob(bucket, provincial_string, UPLOAD_PROVINCIAL)

def main():
    print("Getting data from spreadsheet...")

    confirmed, recovered, dead = get_spreadsheet_data()

    print("Geocoding data...")

    confirmed_output = geocode_sheet(confirmed)
    travel_data = get_travel_data(confirmed)
    provincial_data = get_provincial_totals(confirmed_output, recovered, dead)
    print("Outputting data to file...")
    output_json(confirmed_output, travel_data, provincial_data)
    print("Done")
