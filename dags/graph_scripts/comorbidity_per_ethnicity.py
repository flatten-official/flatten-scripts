import pandas as pd
from google.cloud import storage
from io import StringIO
import os

GCS_DOWNLOAD = os.environ.get('GCS_DOWNLOAD')
DOWNLOAD_BLOB_NAME = "flatten-form-data-v1/1587860903762-flatten-form-data-v1.csv"

GCS_UPLOAD = os.environ.get('GCS_UPLOAD')
UPLOAD_BLOB_NAME = "comorbidity_per_ethnicity.csv"

ethnicity_corrections = {
    "na": "preferNotToAnswer",
    "nan": "preferNotToAnswer",
    "black": "black/african",
    "hispanic": "hispanic/latino",
    "asian": "east asian",
    "nativeamerican": "firstNations"
}

condition_corrections = {
    "cancer": "activeCancer",
    "nan": "noneOfTheAbove",
    "lhypertension": "highBloodPressure",
    "antecedents davc": "historyOfStroke"
}

ethnicities = ["preferNotToAnswer", "east asian", "caucasian", "pacificislander", "firstNations",\
        "black/african", "hispanic/latino", "metis", "south asian", "other"]
conditions = ["noneOfTheAbove", "diabetes", "heartDisease", "historyOfStroke", "immunocompromised",\
             "activeCancer", "highBloodPressure", "other", "kidneyDisease", "breathingProblems"]

def download_blob():
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket(GCS_DOWNLOAD)
    blob = bucket.blob(DOWNLOAD_BLOB_NAME)
    flatten_csv = blob.download_as_string()

    return flatten_csv

def upload_blob(ethnicity_probable):
    """Uploads a file to the bucket."""
    # puts the csv into temp storage to be extracted
    temp_storage = StringIO()
    ethnicity_probable.to_csv(temp_storage)
    temp_storage.seek(0)

    # create bucket and blob
    bucket = storage.Client().bucket(GCS_UPLOAD)
    blob = bucket.blob(UPLOAD_BLOB_NAME)

    # upload from temp storage
    blob.upload_from_file(temp_storage, content_type="text/csv")

    print(
        "File {} uploaded to {}.".format(
            UPLOAD_BLOB_NAME, GCS_UPLOAD
        )
    )

def main():
    # load csv bytes object into flatten_csv variable
    flatten_csv = download_blob()

    # convert to a format readable by pd.read_csv method
    s = str(flatten_csv,'utf-8')
    data = StringIO(s) 
    form_data = pd.read_csv(data)

    comorbidity_and_ethnicity = form_data[["ethnicity", "conditions"]]
    comorbidity_and_ethnicity = comorbidity_and_ethnicity.fillna("nan")
    
    condition_dict = {condition: 0 for condition in conditions}
    accumulator = {ethnicity: {"total": 0, "conditions": condition_dict.copy()}.copy() for ethnicity in ethnicities}

    for index, row in comorbidity_and_ethnicity.iterrows():
        print(index)
        # extract all of the person's conditions into a list
        indiv_ethnicities = row.ethnicity.split(";")
        indiv_conditions = row.conditions.split(";")
        
        # filter out the trolls
        right_amount_of_conditions = len(indiv_conditions) < 5
        conditions_make_sense = not("noneOfTheAbove" in indiv_conditions and len(indiv_conditions) > 1)
        
        if right_amount_of_conditions and conditions_make_sense:
            # add to the total ethnicity count
            for indiv_ethnicity in indiv_ethnicities:
                corrected_ethnicity = ethnicity_corrections[indiv_ethnicity] if ethnicity_corrections.get(indiv_ethnicity) else indiv_ethnicity
                accumulator[corrected_ethnicity]["total"] += 1
                if corrected_ethnicity == "other":
                    print(accumulator[corrected_ethnicity]["total"])
            
                # add all the health conditions
                condition_accumulator = accumulator[corrected_ethnicity]["conditions"]
                for indiv_condition in indiv_conditions:
                    corrected_condition = condition_corrections[indiv_condition] if condition_corrections.get(indiv_condition) else indiv_condition
                    condition_accumulator[corrected_condition] += 1
    
    # convert to a format that can be converted into a csv
    data = {ethnicities: [numbers / conditions["total"] for condition, numbers in conditions["conditions"].items()] for ethnicities, conditions in accumulator.items()}

    conditions_per_ethnicity = pd.DataFrame.from_dict(
        data, 
        orient="index", 
        columns=["noneOfTheAbove", "diabetes", "heartDisease", "historyOfStroke", "immunocompromised",\
             "activeCancer", "highBloodPressure", "other", "kidneyDisease", "breathingProblems"]
    )

    # add a total responses column to the csv
    totals = [values["total"] for  values in accumulator.values()]
    conditions_per_ethnicity["totalResponses"] = totals

    upload_blob(conditions_per_ethnicity)

if __name__ == "__main__":
    main()