'''
covidOntario.py
A script to get up to date Covid-19 Data from the public health units of Ontario.
Author: Ivan Nesterovic
Born: 2020-03-25
'''

## NOTE: Some Public Health units have put their data in text so text parsing is necessary, this may break and require checking over.

import bs4
import requests
import json
from word2number import w2n
from datetime import date

def getSoup(region):
    urlRegion = {
        'Algoma': "http://www.algomapublichealth.com/disease-and-illness/infectious-diseases/novel-coronavirus/",
        'Brant County': "https://www.bchu.org/ServicesWeProvide/InfectiousDiseases/Pages/coronavirus.aspx",
        'Chatham Kent': "https://ckphu.com/current-situation-in-chatham-kent-and-surrounding-areas/",
        'Durham': "https://www.durham.ca/en/health-and-wellness/novel-coronavirus-update.aspx#Status-of-cases-in-Durham-Region",
        'Eastern Ontario': "https://eohu.ca/en/my-health/covid-19-status-update-for-eohu-region",
        'Grey Bruce': None,
        'Halimand Norfolk': None,
        'Haliburton, Kawartha, Pine Ridge District': "https://www.hkpr.on.ca/covid-19-2/covid-19/",
        'Halton': "https://www.halton.ca/For-Residents/Immunizations-Preventable-Disease/Diseases-Infections/New-Coronavirus",
        'Hamilton': "https://www.hamilton.ca/coronavirus/status-cases",
        "Hastings and Prince Edward Counties": "https://hpepublichealth.ca/the-novel-coronavirus-2019ncov/",
        "Huron and Perth": "https://www.hpph.ca/en/news/coronavirus-covid19-update.aspx#COVID-19-in-Huron-and-Perth",
        "Kingston Frontenac Lennox and Addington": "https://www.kflaph.ca/en/healthy-living/novel-coronavirus.aspx",
        "Lambton": "https://lambtonpublichealth.ca/2019-novel-coronavirus/",
        "Leeds, Grenville and Lanark": "https://healthunit.org/coronavirus/",
        "London and Middlesex County": "https://www.healthunit.com/novel-coronavirus",
        "Niagara Region": "https://www.niagararegion.ca/health/covid-19/default.aspx",
        "North Bay Parry Sound": "https://www.myhealthunit.ca/en/health-topics/coronavirus.asp",
        "Northwestern Ontario": "https://www.nwhu.on.ca/Pages/coronavirus.aspx",
        "Ottawa": "https://www.ottawapublichealth.ca/en/reports-research-and-statistics/la-maladie-coronavirus-covid-19.aspx#Ottawa-COVID-19-Case-Details-",
        "Peel": "https://www.peelregion.ca/coronavirus/#cases",
        "Peterborough": "https://www.peterboroughpublichealth.ca/your-health/diseases-infections-immunization/diseases-and-infections/novel-coronavirus-2019-ncov/local-covid-19-status/",
        "Porcupine": "http://www.porcupinehu.on.ca/en/your-health/infectious-diseases/novel-coronavirus/",
        "Sudbury": "https://www.phsd.ca/health-topics-programs/diseases-infections/coronavirus/current-status-covid-19",
        "Renfrew County": "https://www.rcdhu.com/novel-coronavirus-covid-19-2/",
        "Simcoe Muskoka": "http://www.simcoemuskokahealthstats.org/topics/infectious-diseases/a-h/covid-19#Confirmed",
        "Southwestern": "https://www.swpublichealth.ca/content/community-update-novel-coronavirus-covid-19",
        "Thunder Bay": "https://www.tbdhu.com/coronavirus#",
        "Timiskaming": "http://www.timiskaminghu.com/90484/COVID-19",
        "Toronto": "https://www.toronto.ca/home/covid-19/",
        "Waterloo": "https://www.regionofwaterloo.ca/en/health-and-wellness/positive-cases-in-waterloo-region.aspx",
        "Wellington-Dufferin-Guelph": "https://www.wdgpublichealth.ca/your-health/covid-19-information-public/assessment-centre-and-case-data",
        "Windsor-Essex County": "https://www.wechu.org/cv/local-updates",
        "York": "https://www.york.ca/wps/portal/yorkhome/health/yr/infectiousdiseasesandprevention/covid19/covid19/!ut/p/z1/jZDfT4MwEMf_Fh94lB6Mjc63ijrKtmBi3LAvpoMOMKwlLYPEv95Ol5glit7D5e7yuR_fQwxliEne1yXvaiV5Y_MXNnulZEHjeAlJGuAICKQk8UMM93MPbT8B-MUIIPaf_hGAjY9P_lpgFfh6Ha1LxFreVde13CuUWS9yq_FoitoIboThsmi16IU8KUdZrvq68ObfwRaxy1WLJxwA3SQh2XgpBHRyBnw_mMVeBAnEKQb6ED5O73DswdI_A-Nqykbtvh5P5G6C7dla7IUW2j1qW666rjU3DjgwDINbKlU2ws3VwYGfWiplOpRdkqg9PGfvq9s5fZs2_YpcfQDml0gV/dz/d5/L2dBISEvZ0FBIS9nQSEh/#.XnvckW57lTY"
    }
    page = requests.get(urlRegion[region])
    return bs4.BeautifulSoup(page.content, 'html.parser')

def getAlgomaData():
    soup = getSoup('Algoma')
    table = soup.find("table", {'style': "width: 300px; height: 25px; float: left;" })
    algomaData = {}
    for row in table.find_all("tr"):
        dataRow = [cell.get_text(strip=True) for cell in row.find_all("td")]
        algomaData[dataRow[0]] = int(dataRow[1])
    return algomaData

def getBrantCountyData():
    soup = getSoup('Brant County')
    tables = soup.find_all("table", {"class": "ms-rteTable-default"})
    brantCountyData = {}
    rows = []
    for table in tables:
        for row in table.find_all("tr"):
            rows.append([cell.get_text(strip=True) for cell in row.find_all("td")])
    brantCountyData["Tested"] = int(rows[1][1][1:])
    brantCountyData["Positive"] = int(rows[0][1][1:])
    return brantCountyData

def getChathamKentData():
    soup = getSoup('Chatham Kent')
    chathamKentData = {}
    table1 = soup.find("table", {"id": "tablepress-3"})
    table2 = soup.find("table", {"id": "tablepress-4"})
    rows1 = []
    rows2 = []
    for row in table1.find_all("tr"):
        rows1.append([cell.get_text(strip=True) for cell in row.find_all("td")])
    for row in table2.find_all("tr"):
        rows2.append([cell.get_text(strip=True) for cell in row.find_all("td")])
    chathamKentData["Tested"] = int(rows2[5][1])
    chathamKentData["Positive"] = int(rows1[1][1])
    chathamKentData["Pending"] = int(rows2[6][1])
    chathamKentData["Negative"] = chathamKentData["Tested"] - chathamKentData["Positive"] - chathamKentData["Pending"]
    return chathamKentData

def getDurhamData():
    soup = getSoup('Durham')
    table = soup.find("table", {"class": "datatable"})
    durhamData = {}
    durhamData["Positive"] = len(table.find_all("tr")) - 1
    return durhamData

def getEasternOntarioData():
    soup = getSoup("Eastern Ontario")
    table = soup.find("table", {"class": "table table-bordered"})
    easternOntarioData = {}
    easternOntarioData["Positive"] = len(table.find_all("tr")) - 1
    return easternOntarioData

#TODO find the data for these regions ------------------------------

# def getGreyBruceData():

# def getHalimandNorfolkData():

#-------------------------------------------------------------------

def getHaliburtonKawarthaPineRidgeData():
    soup = getSoup('Haliburton, Kawartha, Pine Ridge District')
    table = soup.find("table", {"class": "wp-block-advgb-table aligncenter advgb-table-frontend is-style-stripes"})
    positive = int(table.find_all("tr")[1].find_all("td")[-1].get_text(strip=True))
    return {"Positive": positive}

def getHaltonData():
    soup = getSoup('Halton')
    data = {"Positive": len(soup.find("table", {"class": "table table-striped"}).find_all("tr")) - 1}
    return data

def getHamiltonData():
    soup = getSoup("Hamilton")
    div = soup.find("div", {"class": "coh-column fourth first"})
    data = {"Positive": int(div.find("p").find("strong").text.split()[-1])}
    return data
    
def getHastingsPrinceEdwardData():
    soup = getSoup("Hastings and Prince Edward Counties")
    data = {"Positive": len(soup.find("table", {"class": "has-subtle-pale-blue-background-color has-background"}).find_all("tr")) - 1}
    return data

def getHuronData():
    soup = getSoup("Huron and Perth")
    table = soup.find("table", {"style": "width: 80%;"})
    data = {}
    rows = table.find_all("tr")
    headers = [cell.get_text(strip=True).split()[-1].title() for cell in rows[0].find_all("th")]
    elems = [int(cell.get_text(strip=True)) for cell in rows[1].find_all("td")]
    for i in range(len(headers)):
        data[headers[i]] = elems[i]
    return data

def getKingstonFrontenacLennoxAddingtonData():
    soup = getSoup("Kingston Frontenac Lennox and Addington")
    table = soup.find("table", {"class": "Left datatable"})
    rows = table.find_all("tr")
    data = {}
    for i in range(len(rows) - 1):
        cells = [cell for cell in rows[i].find_all("td")]
        if i == 0:
            data["Positive"] = int(cells[1].get_text(strip=True))
        elif i == 1:
            data["Negative"] = int(cells[1].get_text(strip=True))
        elif i == 2:
            data["Pending"] = int(cells[1].get_text(strip=True))
        elif i == 3:
            data["Tested"] = int(cells[1].get_text(strip=True))
    return data


## for this one pending and negative results are also available but text parsing is necessary, might add later
def getLambtonData():
    soup = getSoup("Lambton")
    table = soup.find("table", {"class": "wp-block-table"})
    cases = int(table.find_all("tr")[1].find_all("td")[1].get_text(strip=True))
    return {"Positive": cases}

##NOTE: currently has no cases so they haven't set up a proper site so this will be done later
def getLeedsGrenvilleLanarkData():
    soup = getSoup("Leeds, Grenville and Lanark")
    words = soup.find_all("div", {"class": "accordion-body"})[0].find("p").get_text(strip=True).split()
    for word in words[::-1]:
        try:
            if ord(word[0]) >= 65:
                cases = w2n.word_to_num(word)
                break
        except:
            pass
    return {"Positive": cases}
    # if(soup.find_all("div", {"class": "accordion-body"})[0].find("p").get_text(strip=True) == "There have been many tests for COVID-19 conducted on people in our community. As of March 26 at 11:00am one test has been positive."):
    #     return {"Positive": 1}
    # raise Exception(NameError)

def getMiddlesexLondonData():
    soup = getSoup("London and Middlesex County")
    table = soup.find_all("table")[0]
    return {"Positive": len(table.find_all("tr")) - 1}

def getNiagaraData():
    soup = getSoup('Niagara Region')
    cases = int(soup.find("strong", {"id": "strCaseNumbers"}).get_text(strip=True))
    return {"Positive": cases}

def getNorthBayParrySoundData():
    soup = getSoup("North Bay Parry Sound")
    table = soup.find("table", {"class": "datatable"})
    rows = table.find_all("tr")
    data = {}
    for i in range(len(rows)):
        dataRow = [cell.get_text(strip=True) for cell in rows[i].find_all("td")]
        if (i == 3):
            data["Tested"] = int(dataRow[1])
        else:
            data[dataRow[0].split()[0]] = int(dataRow[1])
    return data

##NOTE this will probably have to be changed as the situation develops
def getNorthWesternData():
    soup = getSoup("Northwestern Ontario")
    return {"Positive": w2n.word_to_num(soup.find_all("p", {"class": "ms-rteElement-P ms-rteThemeForeColor-2-0"})[1].find("strong").get_text().split()[0])}

def getOttawaData():
    soup = getSoup("Ottawa")
    text = soup.find("p", {"class": "largeButton-Yellow"}).find("strong").get_text(strip=True).split()
    for block in text[::-1]:
        try:
            cases = int(block)
            break
        except:
            pass
    return {"Positive": cases}

def getPeelData():
    soup = getSoup("Peel")
    table = soup.find("table", {"class": "charttable white grid row-hover half margin_top_20"})
    cases = int(table.find_all("tr")[-1].find_all("td")[1].get_text(strip=True))
    return {"Positive": cases}

def getPeterboroughData():
    soup = getSoup("Peterborough")
    lines = soup.find_all("p")[2].get_text().split("\n")
    data = {}
    for i in range(len(lines) - 1):
        head = ""
        if (i == 0):
            head = "Positive"
        elif (i == 1):
            head = "Negative"
        else:
            head = "Pending"
        data[head] = int(lines[i].split()[-1])
    tested = 0
    for value in data.values():
        tested+=value
    data["Tested"] = tested
    return data

def getPorcupineData():
    soup = getSoup("Porcupine")
    table = soup.find("table")
    data = {}
    for row in table.find_all("tr"):
        cells = [row.find("th").get_text(strip=True), row.find("td").get_text(strip=True)]
        if cells[0].split()[0] == "Tests":
            data["Tested"] = int(cells[1])
        else:
            data[cells[0].split()[0]] = int(cells[1])
    return data

def getSudburyData():
    soup = getSoup("Sudbury")
    table = soup.find("table", {"id": "tablepress-1409"})
    cells = [row.find("td", {"class": "column-2"}) for row in table.find_all("tr")]
    return {"Negative": int(cells[2].get_text(strip=True)), 
            "Pending": int(cells[3].get_text(strip=True)), 
            "Positive": int(cells[4].get_text(strip=True)), 
            "Resolved": int(cells[5].get_text(strip=True)),
            "Tested": int(cells[6].get_text(strip=True))}

##NOTE: No cases so no proper website yet, will likely need to be changed soon
def getRenfrewCountyData():
    soup = getSoup("Renfrew County")
    interestingText = soup.find("div", {"id": "collapse-5"}).find_all("p")[1].get_text(strip=True)
    if interestingText == "March 25, 2019 –Renfrew County and District Health Unit (RCDHU) confirms the first positive laboratory confirmed case of novel coronavirus 2019 (COVID-19) in the region. A woman in her 90s developed symptoms and was tested by Pembroke Regional Hospital (PRH) on March 23,2020. She is currently an inpatient at PRH.":
       return {"Positive": 1}
    raise Exception(NameError)

def getSimcoeMuskokaData():
    soup = getSoup("Simcoe Muskoka")
    table = soup.find_all("table")[0]
    return {"Positive": len(table.find_all("tr")) - 1}

def getSouthwesternData():
    soup = getSoup("Southwestern")
    table = soup.find("table")
    return {"Positive": len(table.find_all("tr")) -1}

def getThunderBayData():
    soup = getSoup("Thunder Bay")
    table = soup.find("table")
    data = {}
    for row in table.find_all("tr"):
        cells = [cell.get_text(strip=True) for cell in row.find_all("td")]
        if (cells[0].split()[0] == "Tests"):
            data["Testing"] = int(cells[1])
        else:
            data[cells[0].split()[0]] = int(cells[1])
    return data

def getTimiskamingData():
    soup = getSoup("Timiskaming")
    table = soup.find("table")
    data = {}
    for row in table.find_all("tr"):
        dataRow = [cell.get_text(strip=True) for cell in row.find_all("td")]
        data[dataRow[0]] = int(dataRow[1])
    return data

def getTorontoData():
    soup = getSoup("Toronto")
    paragraph = soup.find("div", {"class": "pagecontent"}).find_all("p")[3].get_text(strip=True)
    return {"Positive": int(paragraph.split()[5])}

def getWaterlooData():
    soup = getSoup("Waterloo")
    cases = 0
    table = soup.find("table", {"class": "datatable"})
    rows = table.find_all("tr")
    for i in range(1, len(rows)):
        caseNum = rows[i].find("td").get_text(strip=True)
        if (caseNum[-1] != '*'):
            cases+=1
    return {"Positive": cases}

def getWellingtonDufferinGuelphData():
    soup = getSoup("Wellington-Dufferin-Guelph")
    tables = soup.find_all("table")
    cases = len(tables[0].find_all("tr")) - 1
    tested = 0
    for i in range(1,3):
        tested += int(tables[i].find_all("tr")[1].find_all("td")[1].get_text(strip=True))
    return {"Positive": cases, "Tested": tested}

def getWindsorEssexCountyData():
    soup = getSoup("Windsor-Essex County")
    divs = soup.find_all("div", {'class': "well"})
    nums = []
    for div in divs[:5]:
        nums.append(div.find_all("p")[1].get_text(strip=True))
    positive = int(nums[0])
    tested = int(nums[3])
    pending = int(nums[4])
    return {"Positive": positive, "Tested": tested, "Pending": pending, "Negative": tested-positive-pending}

def getYorkData():
    soup = getSoup("York")
    table = soup.find("table", {"dir": "ltr"})
    return {"Positive": len(table.find_all("tr")) - 1}
    
def main():

    covidOntario = {}
    covidOntario['Algoma'] = getAlgomaData()
    covidOntario['Brant County'] = getBrantCountyData()
    covidOntario['Chatham Kent'] = getChathamKentData()
    covidOntario['Durham'] = getDurhamData()
    covidOntario['Eastern Ontario'] = getEasternOntarioData()
    #
    #
    covidOntario['Haliburton Kawartha Pine Ridge'] = getHaliburtonKawarthaPineRidgeData()
    covidOntario['Halton'] = getHaltonData()
    covidOntario['Hamilton'] = getHamiltonData()
    covidOntario["Hastings Prince Edward"] = getHastingsPrinceEdwardData()
    covidOntario["Huron and Perth"] = getHuronData()
    covidOntario["Kingston Frontenac Lennox and Addington"] = getKingstonFrontenacLennoxAddingtonData()
    covidOntario["Lambton"] = getLambtonData()
    covidOntario['Leeds, Grenvile and Lanark'] = getLeedsGrenvilleLanarkData()
    covidOntario['Middlesex-London'] = getMiddlesexLondonData()
    covidOntario['Niagara Region'] = getNiagaraData()
    covidOntario['North Bay Parry Sound'] = getNorthBayParrySoundData()
    covidOntario['Northwestern Ontario'] = getNorthWesternData()
    covidOntario["Ottawa"] = getOttawaData()
    covidOntario["Peel"] = getPeelData()
    covidOntario["Peterborough"] = getPeterboroughData()
    covidOntario["Porcupine"] = getPorcupineData()
    #covidOntario["Renfrew County"] = getRenfrewCountyData()
    covidOntario["Simcoe Muskoka"] = getSimcoeMuskokaData()
    covidOntario["Southwestern"] = getSouthwesternData()
    covidOntario["Sudbury"] = getSudburyData()
    covidOntario["Thunder Bay"] = getThunderBayData()
    covidOntario["Timiskaming"] = getTimiskamingData()
    covidOntario["Toronto"] = getTorontoData()
    covidOntario["Waterloo"] = getWaterlooData()
    covidOntario["Wellington-Dufferin-Guelph"] = getWellingtonDufferinGuelphData()
    covidOntario["Windsor-Essex County"] = getWindsorEssexCountyData()
    covidOntario["York"] = getYorkData()
    
    sum = 0
    for value in covidOntario.values():
        sum += value["Positive"]
    print(sum)

    with open(f"covidOntario{date.today().isoformat()}.json", 'w') as jsonFile:
        json.dump(covidOntario, jsonFile, indent=1)

if __name__ == '__main__':
    main()