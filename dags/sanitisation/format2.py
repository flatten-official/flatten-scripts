import pandas as pd
import utils.gcp_helpers as bf
from utils.file_utils import load_fsa_to_hr, csv_map
from google.cloud import storage
from utils import config
import itertools

age_groups = ["<18", "18-25", "26-34", "35-44", "45-54", "55-64", "65-74", ">75"]
household_sizes = ["1", "2", "3", "4", ">=5"]
symptoms = ['fever', 'chills', 'shakes', 'cough', 'shortnessOfBreath', 'diarrhea', 'runnyNose', 'soreThroat', 'lossOfSmellTaste', 
            'stomachPainCramps', 'noneOfTheAbove', 'other']
COLUMNS = (
    ['location','date','reports'] + ['reports among ' + age_group for age_group in age_groups] +
    ['reports where household size is ' + size for size in household_sizes] + ['reports of '+ symptom for symptom in symptoms] +
    [f'reports of {symptom} among {age}' for age, symptom in list(itertools.product(age_groups, symptoms))] +
    ["reports of any 2 symptoms and live in household size " + size for size in household_sizes] +
    ["reports of any 3 symptoms and live in household size " + size for size in household_sizes]
)
fsa_province = {
    'A': 'NL',
    'B': 'Nova Scotia',
    'C': 'PEI',
    'E': 'New Brunswick',
    'G': 'Quebec',
    'H': 'Quebec',
    'J': 'Quebec',
    'K': 'Ontario',
    'L': 'Ontario',
    'M': 'Ontario',
    'N': 'Ontario',
    'P': 'Ontario',
    'R': 'Manitoba',
    'S': 'Saskatchewan',
    'T': 'Alberta',
    'V': 'BC',
    'Y': 'Yukon',
    '0A': 'Nunavut',
    '0B': 'Nunavut',
    '0C': 'Nunavut',
    '0E': 'NWT',
    '1A': 'NWT',
    '0G': 'NWT'
}

def try_convert(row, dic, field):
    try:
        return csv_map(row, dic, field)['health_region']
    except TypeError:
        return None

def try_province(row, dic=fsa_province, field="fsa"):
    try:
        if row[field][0] == 'X':
            return dic[row[field][1:]]
        else:
            return dic[row[field][0]]
    except KeyError:
        return None
    

def to_health_region_prov(df, dic, field):
    df['health_region'] = df.apply(lambda row: try_convert(row, dic, field), axis=1)

def to_province(df):
    df['province'] = df.apply(lambda row: try_province(row), axis=1)

def main():

    vars = config.load_name_config('f2')
    #print(vars)
    #df = bf.get_csv(vars['CSV_BUCKET'], vars['PREFIX'])
    df = bf.get_csv('flatten-dataset', 'flatten-form-data-v1')
    df = df[df['country'] == 'ca']
    fsa_hr = load_fsa_to_hr()
    to_province(df)
    to_health_region_prov(df, fsa_hr, "fsa")
    rows_prov = []
    rows_hr = []
    for region in (list(df.province.unique()) + list(df.health_region.unique())):
        if region in fsa_province.values():
            df_r = df[df['province'] == region]
            rows = rows_prov
        else:
            df_r = df[df['health_region'] == region]
            rows = rows_hr
        for date in df.date.unique():
            df_d = df_r[df_r['date'] == date]
            print(date)
            row = ([region, date, len(df_d)] + [sum(df_d['age'] == age) for age in age_groups] + 
                   [sum(df_d['people_in_household'] == x) for x in [1,2,3,4]] + [sum(df_d['people_in_household'] >= 5)] +
                   [sum(df_d['symptoms'].str.contains(symptom, na=False)) for symptom in symptoms] +
                   [sum((df_d['symptoms'].str.contains(symptom, na=False)) & (df_d['age'] == age)) for age, symptom in list(itertools.product(age_groups, symptoms))] +
                   [sum((df_d['people_in_household'] == x) & (df_d['symptoms'].str.count(";") == 1)) for x in [1,2,3,4]] +
                   [sum((df_d['people_in_household'] >= 5) & (df_d['symptoms'].str.count(";") == 1))] +
                   [sum((df_d['people_in_household'] == x) & (df_d['symptoms'].str.count(";") == 2)) for x in [1,2,3,4]] +
                   [sum((df_d['people_in_household'] >= 5) & (df_d['symptoms'].str.count(";") == 2))]
                   )
            rows.append(row)
    with open('/Users/ivannesterovic/Desktop/provinces_format2.csv', 'w') as file:
        file.write(",".join(COLUMNS) + "\n")
        for row in rows_prov:
            file.write(",".join([(lambda x: str(x))(x) for x in row]) + "\n")
    with open('/Users/ivannesterovic/Desktop/hrs_format2.csv', 'w') as file:
        file.write(",".join(COLUMNS) + "\n")
        for row in rows_hr:
            file.write(",".join([(lambda x: str(x))(x) for x in row]) + "\n")

main()


