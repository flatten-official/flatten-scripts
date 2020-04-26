from google.cloud import datastore, storage
from sanitisation.sanitisation import Sanitisor
from utils.file_utils import load_keys
from utils.time import get_string_date
from gcs.bucket_functions import upload_blob
import os
import json

DS_NAMESPACE = os.environ['DS_NAMESPACE']
DS_KIND = 'PaperformSubmission-so'

GCS_BUCKET = "flatten-staging-271921.appspot.com"
UPLOAD_FILE = "test_somalia.json"

N_CHILDREN = 6
N_EXTRA = 6
INDIVIDUAL_KEYS = ["mother_report", "father_report"] + [f"child_{n}_report" for n in range(1, N_CHILDREN+1)] + [f"extra_{n}_report" for n in range(1, N_EXTRA+1)]


def case_checker(individual):
    potential = any(item in individual for item in ["fever", "cough", "shortnessOfBreath"])
    vulnerable = any(item in individual for item in ["diabetes", "heartDisease", "breathingProblems"])
    return potential, vulnerable

def process_response(response, response_keys):
    individuals = {}
    potential, vulnerable, reports = 0, 0, 0
    # deaths = int(response_keys["number_deaths"])
    print(response_keys)
    deaths = 0
    for key in INDIVIDUAL_KEYS:
        individual_response = [Sanitisor.normalise_property(v) for v in response[key]["value"]]

        if individual_response == []:
            continue

        response_mapped = [response_keys[ans] if ans in response_keys else ans for ans in individual_response]
        print(individual_response)
        print(response_mapped)
        potential, vulnerable = case_checker(response_mapped)
        potential += 1 if potential else 0
        vulnerable += 1 if vulnerable else 0
        reports += 1
        individuals[key] = response_mapped
    aggregated_data = {
        "deaths": deaths,
        "potential": potential,
        "risk": vulnerable,
        "number_reports": reports
    }
    return individuals, aggregated_data


def add_to_dict(existing, new):
    for key in new:
        existing[key] += new[key]

def run_form_data_scraping():
    datastore_client = datastore.Client(namespace=DS_NAMESPACE)
    query = datastore_client.query(kind=DS_KIND)

    keys = load_keys(join_data_folder=False)
    print(keys)
    keys_reversed = {
        Sanitisor.normalise_property(prop): question
        for question, lang in keys.items()
        for mappings in lang.values()
        if 'so' in lang
        for prop in lang['so']
    }
    print(keys_reversed)

    data = {
        "region": {
        },
        "region_time_series": {
            "all_reports": {}
        }
    }
    for entity in query.fetch():
        print(entity)
        individuals, aggregated_data = process_response(entity["data"], keys_reversed)
        district = entity["data"]["district"]["value"]
        day = get_string_date(entity["timestamp"])

        if not district in data["region"]:
            data["region"][district] = {}
        if not district in data["region_time_series"]:
            data["region_time_series"][district] = {}


        try:
            add_to_dict(data["region"][district][day], aggregated_data)
        except KeyError:
            data["region"][district][day] = aggregated_data
        try:
            add_to_dict(data["region_time_series"][district][day], aggregated_data)
        except KeyError:
            data["region_time_series"][district][day] = aggregated_data
        try:
            add_to_dict(data["region_time_series"]["all_reports"][day], aggregated_data)
        except KeyError:
            data["region_time_series"]["all_reports"][day] = aggregated_data

    print(data)
    json_str = json.dumps(data)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    upload_blob(bucket, json_str, UPLOAD_FILE)
