import requests
from datetime import datetime

PROVINCES = ["YUKON", "PRINCE EDWARD ISLAND", "NEWFOUNDLAND AND LABRADOR", "NEW BRUNSWICK", "BRITISH COLUMBIA",\
            "NOVA SCOTIA", "SASKATCHEWAN", "ALBERTA", "MANITOBA", "QUEBEC", "ONTARIO", "NORTHWEST TERRITORIES",\
            "NUNAVUT"]

def convert_unix_timestamp(timestamp):
    """
    Converts unix timestamp (in milliseconds) to a date string

    Parameters:
        timestamp: A long representing milliseconds since Jan. 1st, 1970
    """
    return datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%d')

def main():
    payload = {
        "f": "pjson",
        "where": "SummaryDate>'4/8/2020 12:00PM'",
        "outFields": "OBJECTID, Province, SummaryDate, TotalICU"
    }

    # get # of people in ICU per province from Esri
    response = requests.get(
        "https://services9.arcgis.com/pJENMVYPQqZZe20v/arcgis/rest/services/province_daily_totals/FeatureServer/0/query",
        params=payload,
    )

    # extract the part of the response we care about
    esri_ICU_data = response.json()["features"]

    # parse esri_ICU_data and create the output json file
    ICU_data = {}
    for province in PROVINCES:
        ICU_data[province] = {"Time Series (Daily)": {}}

    for data in esri_ICU_data:
        attributes = data["attributes"]
        date_str = convert_unix_timestamp(attributes["SummaryDate"])

        if attributes["Province"] not in ["REPATRIATED CDN", "REPATRIATED"]:
            ICU_data[attributes["Province"]]["Time Series (Daily)"][date_str] = {"Total ICU": attributes["TotalICU"]}

    print(ICU_data)

if __name__ == "__main__":
    main()
