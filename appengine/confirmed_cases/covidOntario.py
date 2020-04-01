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
import pandas as pd
from word2number import w2n
from datetime import date


def getSoup(region):
    page = requests.get(dispatcher[region]["URL"])
    return bs4.BeautifulSoup(page.content, 'html.parser')


def getAlgomaData():
    soup = getSoup('Algoma')
    table = soup.find("table", {'style': "width: 300px; height: 25px; float: left;"})
    algomaData = {}
    count = 0
    for row in table.find_all("tr"):
        if count == 4:
            break
        dataRow = [cell.get_text(strip=True) for cell in row.find_all("td")]
        algomaData[dataRow[0]] = int(dataRow[1])
        count += 1
    return algomaData


def getBrantCountyData():
    soup = getSoup('Brant')
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
    soup = getSoup('Chatham-Kent')
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
    # table = soup.find("table", {"class": "datatable"})
    # durhamData = {}
    # durhamData["Positive"] = len(table.find_all("tr")) - 1
    paragraph = soup.find("p", {"class": "emphasis-Green"}).get_text(strip=True)
    for word in paragraph.split():
        try:
            cases = int(word)
            return {"Positive": cases}
        except:
            pass
    raise NameError


def getEasternOntarioData():
    soup = getSoup("Eastern")
    table = soup.find("table", {"class": "table table-bordered"})
    easternOntarioData = {}
    easternOntarioData["Positive"] = len(table.find_all("tr")) - 1
    return easternOntarioData


# TODO find the data for these regions ------------------------------

# def getGreyBruceData():

# def getHalimandNorfolkData():

# -------------------------------------------------------------------

def getHaliburtonKawarthaPineRidgeData():
    soup = getSoup('Haliburton Kawartha Pineridge')
    table = soup.find("table", {"class": "wp-block-advgb-table advgb-table-frontend is-style-stripes"})
    positive = int(table.find_all("tr")[1].find_all("td")[-1].get_text(strip=True))
    return {"Positive": positive}


def getHaltonData():
    soup = getSoup('Halton')
    data = {"Positive": len(soup.find("table", {"class": "table table-striped"}).find_all("tr")) - 1}
    return data


def getHamiltonData():
    soup = getSoup("Hamilton")
    div = soup.find("div", {"class": "coh-column fourth first"})
    data = {"Positive": int(div.find("p").find("strong").text.split()[-1][:-1])}
    return data


def getHastingsPrinceEdwardData():
    soup = getSoup("Hastings Prince Edward")
    data = {"Positive": len(
        soup.find("table", {"class": "has-subtle-pale-blue-background-color has-background"}).find_all("tr")) - 1}
    return data


def getHuronData():
    soup = getSoup("Huron Perth")
    table = soup.find("table", {"style": "width: 80%;"})
    data = {}
    rows = table.find_all("tr")
    headers = [cell.get_text(strip=True).split()[-1].title() for cell in rows[0].find_all("th")]
    elems = [int(cell.get_text(strip=True)) for cell in rows[1].find_all("td")]
    for i in range(len(headers)):
        data[headers[i]] = elems[i]
    data['Positive'] = data['Confirmedpositive']
    data.pop('Confirmedpositive', None)
    data['Positive**'] = data['Presumptivepositive*']
    data.pop('Presumptivepositive*', None)
    return data


def getKingstonFrontenacLennoxAddingtonData():
    soup = getSoup("Kingston Frontenac Lennox & Addington")
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
    soup = getSoup("Leeds Grenville Lanark")
    words = soup.find_all("div", {"class": "accordion-body"})[0].find_all("li")[0].get_text(strip=True).split()
    for word in words:
        try:
            cases = int(word)
            break
        except:
            pass
    return {"Positive": cases}


def getMiddlesexLondonData():
    soup = getSoup("Middlesex-London")
    table = soup.find_all("table")[0]
    positive = table.find_all('tr')[1].find_all("td")[1].get_text(strip=True)
    return {"Positive": int(positive)}


def getNiagaraData():
    soup = getSoup('Niagara')
    cases = int(soup.find("strong", {"id": "strCaseNumbers"}).get_text(strip=True))
    return {"Positive": cases}


def getNorthBayParrySoundData():
    soup = getSoup("North Bay Parry Sound")
    tables = soup.find_all("table", {"class": "datatable"})
    positive = tables[0].find_all("tr")[3].find_all("td")[1].get_text(strip=True)
    tested = []
    rows = tables[1].find_all("tr")
    for i in range(1, 4):
        tested.append(rows[i].find_all("td")[1].get_text(strip=True))
    return {"Positive": int(positive), "Negative": int(tested[0]), "Pending": int(tested[1]), "Tested": int(tested[2])}


def getNorthWesternData():
    soup = getSoup("Northwestern")
    table = soup.find("table", {'class': "ms-rteTable-0"})
    rows = table.find_all('tr')
    data = {}
    for i in range(3):
        title = rows[i].find("th").get_text(strip=True).split()[0]
        num = rows[i].find("td").get_text(strip=True)
        if i == 0:
            title = title[2:]
            num = int(num[:-1])
        elif i == 1:
            num = int(num[3:])
        else:
            num = int(num)
        data[title] = num
    data["Tested"] = sum(data.values())
    return data
        

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
        tested += value
    data["Tested"] = tested
    return data


def getPorcupineData():
    soup = getSoup("Porcupine")
    table = soup.find_all("table")[1]
    data = {}
    for row in table.find_all("tr"):
        cells = [row.find("th").get_text(strip=True), row.find_all("td")[0].get_text(strip=True)]
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


def getRenfrewCountyData():
    soup = getSoup("Renfrew")
    divs = soup.find_all('div', {'class': 'col-md-6'})#.get_text(strip=True)
    divs2 = soup.find_all('div', {'class': 'col-md-4'})
    divs += divs2
    nums = []
    for div in divs:
        try:
            text = div.find('div', {'class':'panel-body'}).get_text(strip=True)
            nums.append(int(text))
        except:
            pass
    return {
        "Positive": nums[0],
        "Tested": nums[2],
        "Negative": nums[3],
        "Pending": nums[4]
    }

def getSimcoeMuskokaData():
    soup = getSoup("Simcoe Muskoka")
    table = soup.find_all("table", {"style": "border: currentColor; width: 233.75pt; border-image: none;"})[0]
    return {"Positive": int(table.find_all('tr')[-1].find_all("td")[1].get_text(strip=True))}


def getSouthwesternData():
    soup = getSoup("Southwestern")
    table = soup.find("table")
    return {"Positive": len(table.find_all("tr")) - 1}


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
    paragraph = soup.find("div", {"class": "pagecontent"}).find_all("p")[2].get_text(strip=True)
    return {"Positive": int(paragraph.split(",", 1)[1].split(" ", 1)[0])}


def getWaterlooData():
    soup = getSoup("Waterloo")
    cases = 0
    table = soup.find("table", {"class": "datatable"})
    rows = table.find_all("tr")
    for i in range(1, len(rows)):
        caseNum = rows[i].find("td").get_text(strip=True)
        if (caseNum[-1] != '*'):
            cases += 1
    return {"Positive": cases}


def getWellingtonDufferinGuelphData():
    soup = getSoup("Wellington Dufferin Guelph")
    table = soup.find_all('table')[0]
    positive = table.find_all('tr')[1].find_all('td')[1].get_text(strip=True)
    return {"Positive": int(positive)}


def getWindsorEssexCountyData():
    soup = getSoup("Windsor-Essex")
    divs = soup.find_all("div", {'class': "well"})
    nums = []
    for div in divs[:5]:
        nums.append(div.find_all("p")[1].get_text(strip=True))
    positive = int(nums[0].replace(',', ""))
    tested = int(nums[3].replace(',', ""))
    pending = int(nums[4].replace(',', ""))
    return {"Positive": positive, "Tested": tested, "Pending": pending, "Negative": tested - positive - pending}


def getYorkData():
    df = pd.read_csv(dispatcher["York"]["URL"])
    return {"Positive": len(df)}


dispatcher = {
    "Algoma": {
        "func": getAlgomaData,
        "URL": "http://www.algomapublichealth.com/disease-and-illness/infectious-diseases/novel-coronavirus/current-status-covid-19/"
    },
    "Brant": {
        "func": getBrantCountyData,
        "URL": "https://www.bchu.org/ServicesWeProvide/InfectiousDiseases/Pages/coronavirus.aspx"
    },
    "Chatham-Kent": {
        "func": getChathamKentData,
        "URL": "https://ckphu.com/current-situation-in-chatham-kent-and-surrounding-areas/"
    },
    "Durham": {
        "func": getDurhamData,
        "URL": "https://www.durham.ca/en/health-and-wellness/novel-coronavirus-update.aspx#Status-of-cases-in-Durham-Region"
    },
    "Eastern": {
        "func": getEasternOntarioData,
        "URL": "https://eohu.ca/en/my-health/covid-19-status-update-for-eohu-region"
    },
    "Grey Bruce": {
        "func": None,
        "URL": None
    },
    "Halimand Norfolk": {
        "func": None,
        "URL": None
    },
    "Haliburton Kawartha Pineridge": {
        "func": getHaliburtonKawarthaPineRidgeData,
        "URL": "https://www.hkpr.on.ca/"
    },
    "Halton": {
        "func": getHaltonData,
        "URL": "https://www.halton.ca/For-Residents/Immunizations-Preventable-Disease/Diseases-Infections/New-Coronavirus"
    },
    "Hamilton": {
        "func": getHamiltonData,
        "URL": "https://www.hamilton.ca/coronavirus/status-cases"
    },
    "Hastings Prince Edward": {
        "func": getHastingsPrinceEdwardData,
        "URL": "https://hpepublichealth.ca/the-novel-coronavirus-2019ncov/"
    },
    "Huron Perth": {
        "func": getHuronData,
        "URL": "https://www.hpph.ca/en/news/coronavirus-covid19-update.aspx#COVID-19-in-Huron-and-Perth"
    },
    "Kingston Frontenac Lennox & Addington": {
        "func": getKingstonFrontenacLennoxAddingtonData,
        "URL": "https://www.kflaph.ca/en/healthy-living/novel-coronavirus.aspx"
    },
    "Lambton": {
        "func": getLambtonData,
        "URL": "https://lambtonpublichealth.ca/2019-novel-coronavirus/"
    },
    "Leeds Grenville Lanark": {
        "func": getLeedsGrenvilleLanarkData,
        "URL": "https://healthunit.org/coronavirus/"
    },
    "Middlesex-London": {
        "func": getMiddlesexLondonData,
        "URL": "https://www.healthunit.com/novel-coronavirus"
    },
    "Niagara": {
        "func": getNiagaraData,
        "URL": "https://www.niagararegion.ca/health/covid-19/default.aspx"
    },
    "North Bay Parry Sound": {
        "func": getNorthBayParrySoundData,
        "URL": "https://www.myhealthunit.ca/en/health-topics/coronavirus.asp"
    },
    "Northwestern": {
        "func": getNorthWesternData,
        "URL": "https://www.nwhu.on.ca/covid19/Pages/home.aspx/"
    },
    "Ottawa": {
        "func": getOttawaData,
        "URL": "https://www.ottawapublichealth.ca/en/reports-research-and-statistics/la-maladie-coronavirus-covid-19.aspx#Ottawa-COVID-19-Case-Details-"
    },
    "Peel": {
        "func": getPeelData,
        "URL": "https://www.peelregion.ca/coronavirus/testing/#cases"
    },
    "Peterborough": {
        "func": getPeterboroughData,
        "URL": "https://www.peterboroughpublichealth.ca/your-health/diseases-infections-immunization/diseases-and-infections/novel-coronavirus-2019-ncov/local-covid-19-status/"
    },
    "Porcupine": {
        "func": getPorcupineData,
        "URL": "http://www.porcupinehu.on.ca/en/your-health/infectious-diseases/novel-coronavirus/"
    },
    "Renfrew": {
        "func": getRenfrewCountyData,
        "URL": "https://www.rcdhu.com/novel-coronavirus-covid-19-2/"
    },
    "Simcoe Muskoka": {
        "func": getSimcoeMuskokaData,
        "URL": "http://www.simcoemuskokahealthstats.org/topics/infectious-diseases/a-h/covid-19#Confirmed"
    },
    "Southwestern": {
        "func": getSouthwesternData,
        "URL": "https://www.swpublichealth.ca/content/community-update-novel-coronavirus-covid-19"
    },
    "Sudbury": {
        "func": getSudburyData,
        "URL": "https://www.phsd.ca/health-topics-programs/diseases-infections/coronavirus/current-status-covid-19"
    },
    "Thunder Bay": {
        "func": getThunderBayData,
        "URL": "https://www.tbdhu.com/coronavirus#"
    },
    "Timiskaming": {
        "func": getTimiskamingData,
        "URL": "http://www.timiskaminghu.com/90484/COVID-19"
    },
    "Toronto": {
        "func": getTorontoData,
        "URL": "https://www.toronto.ca/home/covid-19/"
    },
    "Waterloo": {
        "func": getWaterlooData,
        "URL": "https://www.regionofwaterloo.ca/en/health-and-wellness/positive-cases-in-waterloo-region.aspx"
    },
    "Wellington Dufferin Guelph": {
        "func": getWellingtonDufferinGuelphData,
        "URL": "https://www.wdgpublichealth.ca/your-health/covid-19-information-public/status-cases-wdg"
    },
    "Windsor-Essex": {
        "func": getWindsorEssexCountyData,
        "URL": "https://www.wechu.org/cv/local-updates"
    },
    "York": {
        "func": getYorkData,
        "URL": "https://ww4.yorkmaps.ca/COVID19/PublicCaseListing/TableListingExternalData.csv"
    }
}


def main():
    covidOntario = {}
    sum = 0
    for key in dispatcher.keys():
        try:
            data = dispatcher[key]['func']()
            covidOntario[key] = data
            sum += data['Positive']
            print(f"Scraped {key}")
        except:
            print(f"Failed on {key}")
    print(sum)

    with open(f"covidOntario{date.today().isoformat()}.json", 'w') as jsonFile:
        json.dump(covidOntario, jsonFile, indent=1)


if __name__ == '__main__':
    main()
