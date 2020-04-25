import requests
import json

PROVINCES = ["YUKON", "PRINCE EDWARD ISLAND", "NEWFOUNDLAND AND LABRADOR", "NEW BRUNSWICK", "BRITISH COLUMBIA",\
            "NOVA SCOTIA", "SASKATCHEWAN", "ALBERTA", "MANITOBA", "QUEBEC", "ONTARIO", "NORTHWEST TERRITORIES",\
            "NUNAVUT"]

MAPPING = {
    "AB": "ALBERTA",
    "BC": "BRITISH COLUMBIA",
    "SK": "SASKATCHEWAN",
    "MB": "MANITOBA",
    "ON": "ONTARIO",
    "NB": "NEW BRUNSWICK",
    "NL": "NEWFOUNDLAND AND LABRADOR",
    "NS": "NOVA SCOTIA",
    "YT": "YUKON",
    "NT": "NORTHWEST TERRITORIES",
    "PE": "PRINCE EDWARD ISLAND"
}

def main():
    payload = {
        "f": "pjson",
        "where": "1=1",
        "outFields": "Tests, Abbreviation",
        "returnGeometry": False
    }

    # get # of people in ICU per province from Esri
    response = requests.get(
        "https://services9.arcgis.com/pJENMVYPQqZZe20v/arcgis/rest/services/Join_Features_to_Enriched_Population_Case_Data_By_Province_Polygon/FeatureServer/0/query",
        params=payload,
    )

    esri_tested = response.json()
    print(esri_tested)

if __name__ == "__main__":
    main()