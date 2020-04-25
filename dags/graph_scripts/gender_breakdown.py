from google.cloud import storage
from io import StringIO
import pandas as pd
import os

GCS_BUCKET = os.environ.get('GCS_SAVE_BUCKET')

UPLOAD_SEX = "sex.csv"

def get_sex_insights_df():
    form_data = pd.read_csv("gcs/dags/data/flatten.csv")

    yesno_mapping = {"n": 0, "y": 1}
    yesno_columns = ["probable", "vulnerable", "is_most_recent", "fever_chills_shakes", "cough", "shortness_of_breath",\
                    "over_60", "any_medical_conditions", "travel_outside_canada", "contact_with_illness", "covid_positive"]

    # replace things that are yes and no
    replacements = {column: yesno_mapping for column in yesno_columns}
    form_data = form_data.replace(replacements)

    filtered_df = form_data[["sex", "probable"]]
    sex_probable = filtered_df.groupby("sex", as_index=False).mean().sort_values(by="probable", ascending=False)
    sex_probable["count"] = filtered_df.groupby("sex", as_index=False).count()["probable"]

    return sex_probable


def upload_blob(sex_probable):
    """Uploads a file to the bucket."""
    # puts the csv into temp storage to be extracted
    temp_storage = StringIO()
    sex_probable.to_csv(temp_storage)
    temp_storage.seek(0)

    # create bucket and blob
    bucket = storage.Client().bucket(GCS_BUCKET)
    blob = bucket.blob(UPLOAD_SEX)

    # upload from temp storage
    blob.upload_from_file(temp_storage, content_type="text/csv")

    print(
        "File {} uploaded to {}.".format(
            UPLOAD_SEX, GCS_BUCKET
        )
    )

def main():
    print("Getting gender insights...")
    sex_probable = get_sex_insights_df()

    upload_blob(sex_probable)


if __name__ == "__main__":
    main()