import os
import json
import pytz
import pandas as pd

import confirmed_cases.helper as helper
from confirmed_cases.covidOntario import dispatcher

from datetime import datetime
from google.cloud import storage
from googleapiclient.discovery import build
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


NAME_EXCEPTIONS = {
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
    "Zone 1 (Moncton area), New Brunswick": "Moncton, New Brunswick",
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
    'Wellington Dufferin Guelph, Ontario': "Guelph, Ontario",
    'Zone 5 (Campbellton area), New Brunswick': 'Campbellton, New Brunswick',
    'Zone 4 - Central, Nova Scotia': 'Halifax, Nova Scotia',
    'Zone 1 - Western, Nova Scotia': 'Caledonia, Nova Scotia',
    'Haldimand Norfolk, Ontario': 'Haldimand, Ontario',
    'Terres-Cries-de-la-Baie-James, Quebec': 'Chibougamau, Quebec',
    'Gaspésie-Îles-de-la-Madeleine, Quebec': 'Cap-aux-Meules, Quebec',
    'Western, NL': 'Corner Brook, NL',
    'Far North, Saskatchewan': 'Stony Rapids, Saskatchewan',
    'Eastern, NL': 'St. John\'s, NL',
    'Central, NL': 'Gander, NL'
}

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SHEETS_API_KEY = os.environ.get('SHEETS_API_KEY')
GCS_BUCKET = os.environ.get('GCS_SAVE_BUCKET')

SPREADSHEET_ID = '1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo'
SPREADSHEET_CASES = 'Cases'
SPREADSHEET_REC = 'Recovered'
SPREADSHEET_DEATH = 'Mortality'

UPLOAD_CONFIRMED = 'confirmed_data_composer.json'
UPLOAD_TRAVEL = 'travel_data_composer.json'
UPLOAD_PROVINCIAL = 'provincial_data_composer.json'


def get_spreadsheet_data():
    """
    Uses the Google Sheets API to access the database of confirmed cases of
    COVID-19 in Canada
    Credit: https://github.com/ishaberry/Covid19Canada

    Returns Panda data frames for each sheet
    """
    service = build('sheets', 'v4', developerKey=SHEETS_API_KEY)

    # Call the Sheets API and get the content
    sheet = service.spreadsheets()
    cases_data = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SPREADSHEET_CASES).execute().get('values', [])
    rec_data = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SPREADSHEET_REC).execute().get('values', [])
    death_data = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SPREADSHEET_DEATH).execute().get('values',
                                                                                                         [])

    if not (cases_data and rec_data and death_data):
        raise Exception("No data found")

    return format_data(cases_data), format_data(rec_data), format_data(death_data)


def new_confirmed_case_row(region_name, cases, coords):
    """Creates a row object. Separate to avoid typos"""
    return {'name': region_name, 'cases': cases, 'coord': coords}


def get_confirmed_cases(df):
    """
    Uses the spreadsheet and scraper data to assign lat/lon locations
    to each area with a confirmed case
    """

    # Group the health region and province together for geocoding and grouping by case
    df['health_region'] += ', ' + df['province']  # Append province to health region for geocoding
    df_aggregate = df.groupby('health_region').size()  # Aggregate by health province

    # Set up the geocoder, with a rate limiter to deal with timeout issues
    geo_coder = RateLimiter(Nominatim(user_agent="COVIDScript").geocode, min_delay_seconds=.5)

    # Start with ontario data
    confirmed_cases = get_ontario_confirmed_cases(geo_coder)
    confirmed_cases.extend(get_spreadsheet_confirmed_cases(df_aggregate, geo_coder))

    return {'last_updated': get_time_string(df), 'max_cases': int(df_aggregate.max()),
            'confirmed_cases': confirmed_cases}


def get_spreadsheet_confirmed_cases(df, geo_coder):
    confirmed_cases = []

    # Iterate through all the health regions for geocoding
    for region, cases in df.iteritems():
        municipality, province = split_region(region)

        # If Ontario, skip as the scraper will deal with that information
        if province == "Ontario":
            continue

        # Case for a province only included
        if municipality == "Not Reported":
            if province == "Repatriated":
                # TODO Change data scheme to not include coord
                confirmed_cases.append(new_confirmed_case_row(region, cases, ("N/A", "N/A")))
                continue
            region = province  # Region is now just the province

        geo_coder_name = NAME_EXCEPTIONS[region] if region in NAME_EXCEPTIONS else region

        confirmed_cases.append(new_confirmed_case_row(region, cases, get_coords(geo_coder_name, geo_coder)))

    return confirmed_cases


def get_ontario_confirmed_cases(geo_coder):
    confirmed_cases = []

    # Gets ontario data
    for name, value in dispatcher.items():
        name += ", Ontario"

        try:
            cases = value["func"]()['Positive']
        except Exception as e:
            print(f"WARNING: Failed on {name}. {e}")
            continue

        if cases == 0:
            continue

        confirmed_cases.append(new_confirmed_case_row(name, cases, get_coords(
            NAME_EXCEPTIONS[name] if name in NAME_EXCEPTIONS else name, geo_coder
        )))

    return confirmed_cases


def get_time_string(df):
    """
    Provide information on the time of the last update
    """
    dt_string = datetime.now(pytz.timezone('US/Eastern')).strftime("%d/%m/%Y %H:%M").replace('/', '-')
    last_updated = f"Data last accessed at: {dt_string}. Latest case reported on: {df.iloc[-1]['date_report']}."
    return last_updated


def get_coords(name, geo_coder):
    """
    Returns the coordinates of the `name` location using the geo_coder.
    """
    location = geo_coder(name + ', Canada')
    # Default to province if geocoding failed
    if location is None:
        print(f"WARNING: Failed to geocode {name}.")
        location = geo_coder(name.split(", ", 1)[1] + ', Canada')

    return location.latitude, location.longitude


def format_data(spreadsheet_data):
    df = pd.DataFrame(spreadsheet_data)

    # Assign column names to the dataframe
    df.columns = list(df.iloc[3])

    # The first 3 rows of the spreadsheet are information regarding the
    # sheet, so we drop them
    df.drop([0, 1, 2, 3], inplace=True)
    df.index -= 4
    return df


def get_travel_data(df):
    # Collects data
    has_travelled = df.loc[df['travel_yn'] == '1']['travel_history_country']
    not_travelled = df.loc[df['travel_yn'] == '0']['locally_acquired']

    # Counts each instance of a travel location / method of infection and stores in a dict
    travel_data = has_travelled.value_counts().to_dict()
    no_travel_data = not_travelled.value_counts().to_dict()

    # no_travel_data contains both 'Close contact' and 'Close Contact' this is to merge them
    if 'Close contact' in no_travel_data:
        no_travel_data['Close Contact'] += no_travel_data.pop('Close contact')

    # Some values are "France, Germany", these values are split into their countries. We are aware this double counts.
    new_travel_data = {}

    for key, cases in travel_data.items():
        locations = key.split(",")
        for location in locations:
            location = location.strip().title()
            if location not in new_travel_data:
                new_travel_data[location] = 0
            new_travel_data[location] += cases

    total_cases, travel_cases, no_travel_cases = len(df), len(has_travelled), len(not_travelled)

    return {"cases": total_cases,
            "travel_cases": travel_cases,
            "no_travel_cases": no_travel_cases,
            "not_reported": total_cases - travel_cases - no_travel_cases,
            "travel_data": new_travel_data,
            "no_travel_data": no_travel_data}


def split_region(region):
    """
    Returns the municipality and the province
    """
    if region.find(", ") > 0:
        return tuple(region.rsplit(", "))
    else:
        return None, region  # In this case the region is only the province


def get_provincial_totals(output_dict, rec_df, dead_df):
    # creates provincial data dictionary
    provincial_data = {}

    # adds case numbers to their respective province/territory
    for region in output_dict['confirmed_cases']:
        _, province = split_region(region['name'])

        if province not in provincial_data:
            provincial_data[province] = {'cases': 0}

        provincial_data[province]['cases'] += region['cases']

    # Adds recovery and mortality data to each province
    for province in provincial_data.keys():
        # Skip repatriated as there is no data
        if province == 'Repatriated':
            continue

        # This Block Deals with recovery data -----------------------------------------------------
        # selects only the columns we are interested in
        rec_data = rec_df.loc[rec_df['province'] == province].loc[:, ['date_recovered', 'cumulative_recovered']]
        # in a try except since some provinces have no deaths
        # drops not available data
        rec_data = rec_data[rec_data.cumulative_recovered != "NA"]
        # converts counts to integer
        rec_data['cumulative_recovered'] = rec_data['cumulative_recovered'].astype(int)
        # converts to dictionary format with date as key and cumulative count as value
        rec_data = rec_data.set_index('date_recovered')['cumulative_recovered'].to_dict()
        provincial_data[province]['recovered_cumul'] = rec_data

        # This Block Deals with Death data ---------------------------------------------------------
        # selects the column containing date of reported death and counts the number of deaths on that day
        # also converts to dictionary form
        provincial_data[province]['dead_daily'] = dead_df.loc[dead_df['province'] == province][
            'date_death_report'].value_counts(sort=False).to_dict()
        ## gets total recovered and total dead for a province
        provincial_data[province]['total_recovered'] = max(provincial_data[province]['recovered_cumul'].values())
        provincial_data[province]['total_dead'] = sum(provincial_data[province]['dead_daily'].values())
        
    return provincial_data


# Uploads the JSON to the bucket
def write_data_to_bucket(confirmed_out, travel_out, provincial_out):
    bucket = storage.Client().bucket(GCS_BUCKET)
    helper.upload_json(bucket, confirmed_out, UPLOAD_CONFIRMED)
    helper.upload_json(bucket, travel_out, UPLOAD_TRAVEL)
    helper.upload_json(bucket, provincial_out, UPLOAD_PROVINCIAL)


def main():

    print("Getting data from spreadsheet...")
    confirmed, recovered, dead = get_spreadsheet_data()

    print("Geocoding data (this takes a few minutes)...")
    confirmed_output = get_confirmed_cases(confirmed)
    travel_data = get_travel_data(confirmed)
    provincial_data = get_provincial_totals(confirmed_output, recovered, dead)

    print("Uploading files to bucket...")
    write_data_to_bucket(confirmed_output, travel_data, provincial_data)

    print("Done")


if __name__ == "__main__":
    main()
