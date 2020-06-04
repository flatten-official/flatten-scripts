import pandas as pd
import utils.gcp_helpers as bf
from utils.file_utils import load_fsa_to_hr, csv_map
from google.cloud import storage
from utils import config
import itertools
import time
from os.path import expanduser

home = expanduser("~")

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

ACTIVE_PROV = pd.read_csv('https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/active_timeseries_prov.csv')
CASES_PROV = pd.read_csv('https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv')
MORTALITY_PROV = pd.read_csv('https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/mortality_timeseries_prov.csv')
RECOVERED_PROV = pd.read_csv('https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/recovered_timeseries_prov.csv')
TESTING_PROV = pd.read_csv('https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/testing_timeseries_prov.csv')

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

def update_region(row, dicts):
    print(row['symptoms'])
    print(row['people_in_household'])
    age = row['age']
    try:
        size = int(row['people_in_household'])
    except ValueError:
        size = 0
    for dic in dicts:
        dic['reports'] += 1
        try:
            dic[f'reports among {age}'] += 1
        except KeyError:
            pass
        if type(row['symptoms']) == str:
            for symptom in row['symptoms'].split(';'):
                if symptom == 'diarrhee':
                    symptom = 'diarrhea'
                dic[f'reports of {symptom}'] += 1
                dic[f'reports of {symptom} among {age}'] += 1
        if size >= 5:
            dic['reports where household size is >=5']
            if type(row['symptoms']) == str:
                if row['symptoms'].count(";") == 1:
                    dic[f"reports of any 2 symptoms and live in household size >=5"] += 1
                elif row['symptoms'].count(";") == 2:
                    dic[f"reports of any 3 symptoms and live in household size >=5"] += 1
        else:
            try:
                dic[f'reports where household size is {size}'] += 1
                if type(row['symtoms']) == str:
                    if row['symptoms'].str.count(";") == 1:
                        dic[f"reports of any 2 symptoms and live in household size {size}"] += 1
                    elif row['symptoms'].str.count(";") == 2:
                        dic[f"reports of any 3 symptoms and live in household size {size}"] += 1
            except KeyError:
                pass
        

def main():

    vars = config.load_name_config('f2')
    #df = bf.get_csv(vars['CSV_BUCKET'], vars['PREFIX'])
    df = bf.get_csv('flatten-dataset', 'flatten-form-data-v1')
    df = df[df['country'] == 'ca']
    fsa_hr = load_fsa_to_hr()
    to_province(df)
    to_health_region_prov(df, fsa_hr, "fsa")
    df.dropna(subset=['province', 'health_region'])
    data_fsa = {}
    data_prov = {}
    data_hr = {}
    for index, row in df.iterrows():
        fsa = row['fsa']
        prov = row['province']
        hr = row['health_region']
        date = row['date']
        if fsa in data_fsa:
            if date not in data_fsa[fsa]:
                data_fsa[fsa][date] = dict.fromkeys(COLUMNS[2:], 0)
        else:
            data_fsa[fsa] = {date: dict.fromkeys(COLUMNS[2:], 0)}
        if prov in data_prov:
            if date not in data_prov[prov]:
                data_prov[prov][date] = dict.fromkeys(COLUMNS[2:], 0)
        else:
            data_prov[prov] = {date: dict.fromkeys(COLUMNS[2:], 0)}
        if hr in data_hr:
            if date not in data_hr[hr]:
                data_hr[hr][date] = dict.fromkeys(COLUMNS[2:], 0)
        else:
            data_hr[hr] = {date: dict.fromkeys(COLUMNS[2:], 0)}
        update_region(row, [data_fsa[fsa][date], data_prov[prov][date], data_hr[hr][date]])

    # Creates the individual dataframes for each tab of the excel form data.
    rows_prov = []
    rows_fsa = []
    rows_hr = []
    for key, value in data_prov.items():
        for kee, val in value.items():
            rows_prov.append([key, kee] + [v for v in val.values()])
    for key, value in data_fsa.items():
        for kee, val in value.items():
            rows_fsa.append([key, kee] + [v for v in val.values()])
    for key, value in data_hr.items():
        for kee, val in value.items():
            rows_hr.append([key, kee] + [v for v in val.values()])
    form_prov = pd.DataFrame(columns=COLUMNS, data=rows_prov)
    form_hr = pd.DataFrame(columns=COLUMNS, data=rows_hr)
    form_fsa = pd.DataFrame(columns=COLUMNS, data=rows_fsa)
    
    with pd.ExcelWriter(home + '/Desktop/flatten.xlsx') as writer:
        form_prov.to_excel(writer, sheet_name='Flatten - Province')
        form_hr.to_excel(writer, sheet_name='Flatten - Health Region')
        form_fsa.to_excel(writer, sheet_name='Flatten - FSA')

if __name__ == '__main__':
    main()


