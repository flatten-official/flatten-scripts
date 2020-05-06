import utils.gcp_helpers as bf
from utils import config
from utils.file_utils import load_fsa_to_hr, csv_map
from google.cloud import storage
import json
import pandas as pd

UPLOADFILE_NAME = "health_region_data.json"

def try_convert(row, dic, field):
    try:
        return csv_map(row, dic, field)['health_region']
    except TypeError:
        return None

def to_health_region_prov(df, dic, field):
    df['health_region'] = df.apply(lambda row: try_convert(row, dic, field), axis=1)

def health_region_data():
    hr_data = {}
    vars = config.load_name_config("svg_data")
    df = bf.get_csv(vars['CSV_BUCKET'], vars["PREFIX"])
    data = load_fsa_to_hr()
    to_health_region_prov(df, data, "fsa")
    df.dropna(subset=['health_region'], inplace=True)
    for hr in df['health_region'].unique():
        hr_data[hr] = {}
        hr_df = df[df['health_region'] == hr]
        for date in hr_df.date.unique():
            date_df = hr_df[hr_df['date'] == date]
            reports = len(date_df)
            pot = len(date_df[date_df['probable'] == 'y'])
            vul = len(date_df[date_df['vulnerable'] == 'y'])
            fin_sup = len(date_df[date_df['needs'] == 'financialSupport'])
            diabetes = len(date_df[date_df['conditions'].str.contains('diabetes',na=False)])
            hyp = len(date_df[date_df['conditions'].str.contains('highBloodPressure',na=False)])
            hr_data[hr][date] = {"total": reports, "potential": pot, "vulnerable": vul, "financial_support": fin_sup, "diabetes": diabetes, "hypertension": hyp}
    json_str = json.dumps(hr_data)

    storage_client = storage.Client()
    bucket = storage_client.bucket(vars["UPLOAD_BUCKET"])
    bf.upload_blob(bucket, json_str, UPLOADFILE_NAME)
