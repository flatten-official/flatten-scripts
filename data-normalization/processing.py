import pandas as pd
import numpy as np

#census_data = pd.read_csv('processed.csv')


# read the csv file from the data store: flatten-form-data.csv
flatten_data = pd.read_csv('flatten-form-data.csv')

# functions for identifying "at risk" and "potential cases"
def atrisk(answer):
    if answer['q4'] == 'y' or answer['q5'] == 'y':
        return True
    return False

def ispotential(answer):
    if answer['q3'] == 'y':
        return True
    if answer['q1'] == 'y':
        if answer['q2'] == 'y':
            return True
        if answer['q6'] == 'y':
            return True
    if answer['q6'] == 'y':
        if answer['q2'] == 'y':
            return True
        if answer['q3'] == 'y':
            return True
    if answer['q7'] == 'y':
        return True
    return False


# process census data:
# the processed census data with schema: index,FSA,census under 60,census above 60,census total
# is uploaded in this repository called as census.csv
"""
census_data = pd.DataFrame(census_data)
grouped_census = census_data.groupby('GEO_NAME').head(13)
under_60 = grouped_census.groupby('GEO_NAME')['Dim: Sex (3): Member ID: [1]: Total - Sex'].agg(
    lambda x: pd.to_numeric(x, errors='coerce').sum())
total = census_data.groupby('GEO_NAME')['Dim: Sex (3): Member ID: [1]: Total - Sex'].agg(
    lambda x: pd.to_numeric(x, errors='coerce').sum())
save = under_60.to_csv('under60.csv', header=1)
total = pd.read_csv('whats2.csv')
under_60 = pd.read_csv('under60.csv')
save = total.to_csv('whats2.csv', header=True)
df_all = under_60
df_all.columns = ['FSA', 'under 60']
df_all['above 60'] = total['population'] - df_all['under 60']
df_all['above 60'] = df_all['above 60'].astype(int)
df_all['under 60'] = df_all['under 60'].astype(int)
df_all['total'] = total['population'].astype(int)
s = df_all.to_csv('census.csv')
"""
# split flatten data:
flatten_data = pd.DataFrame(flatten_data)
# extract out questions answer (q1,...,q8)
response = flatten_data[flatten_data.columns[3:-1]]
# identify "potential cases" and " cases at risk"
# convert each row to a dict
x = response.to_dict('index')
risk = []
potential = []
for i in x: # x is all the responses in dict format
    this_risk = atrisk(x[i])
    this_potential = ispotential(x[i])
    if this_risk:
        risk += [1]
    else:
        risk += [0]
    if this_potential:
        potential += [1]
    else:
        potential += [0]
# add these two fields
response['risky'] = risk
response['potential'] = potential
# add fsa
response['FSA'] = flatten_data['fsa']
# save to csv
response.to_csv('flatten.csv')
# group by fsa
risks = response.groupby('FSA')['risky'].agg(
    lambda x: pd.to_numeric(x, errors='coerce').sum())
potential = response.groupby('FSA')['potential'].agg(
    lambda x: pd.to_numeric(x, errors='coerce').sum())
new = pd.merge(risks, potential, on='FSA')
new.to_csv('new.csv')
# get count of responses from each area
new2 = response.groupby('FSA')['q1'].agg(['count'])
new3 = pd.merge(new2, new, on='FSA') # index,FSA,count,risky,potential
# read processed census data
census_processed = pd.read_csv('census.csv')
new4 = pd.merge(new3, census_processed, on='FSA')
# save to csv
# new4.to_csv('final_data.csv')
# now the schema is : "idx,FSA,count,risky,potential,index,census under 60,census above 60,census total"
# Normalization: (flatten_potential/flatten_count) * census_total
new4['normalized_potential'] = ((new4['potential']/new4['count']) * (new4['census total'])).round(0).astype(int)
new4['normalized_risky'] = ((new4['risky']/new4['count']) * (new4['census total'])).round(0).astype(int)
# note it's been rounded to integers
# save
new4.to_csv('final_data.csv')


data = pd.read_csv('final_data.csv')
data = data.set_index('FSA')
data = data.to_json('normalized_data.json', orient='index')




