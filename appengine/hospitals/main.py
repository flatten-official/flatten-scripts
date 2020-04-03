import bs4
import requests
import re
import json
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def getSoup(link):
    page = requests.get(link)
    return bs4.BeautifulSoup(page.content, 'html.parser')

def get_hospital_geocodes(hospital_list):
    # initialize geolocator object and set a rate limiter to limit request rates
    geolocator = Nominatim(user_agent="Hospital_Locator", timeout=5)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, error_wait_seconds=3)

    geocode_failures = []
    geocode_successes = []

    # iterate through the hospital list and geocode
    for original_hospital in hospital_list:
        # make a copy of the hospital string
        print(original_hospital)
        hospital = original_hospital

        # try with original string first
        location = geocode(hospital)
        if not location:
            # take out the abbreviation at the end
            hospital = re.sub(r'\([^)]*\)', '', hospital)
            location = geocode(hospital)

        if not location:
            # keep going until either geolocator finds something or sentence runs out of "and" or "&"
            while not location and any(concatenator in hospital.split(" ") for concatenator in {"and", "&"}):
                # split and reverse the string
                splitted_string = hospital.split(" ")
                reversed_string = list(reversed(splitted_string))

                # take out the portion after an "and" or an "&"
                for substr in reversed_string:
                    if substr in ("and", "&"):
                        hospital = hospital.rsplit(substr, 1)[0]
                        break
                location = geocode(hospital)

        if location and "Canada" not in location.address:
            location = geocode("{}, Canada".format(hospital))

        if not location:
            geocode_failures.append(original_hospital)
        else:
            geocode_successes.append({
                "name": original_hospital,
                "location": [location.longitude, location.latitude]
            })

    return {
        "geocode_successes": geocode_successes,
        "geocode_failures": geocode_failures
    }

def main():
    soup = getSoup("https://en.wikipedia.org/wiki/List_of_hospitals_in_Canada")

    tag_list = []

    # find all li tags in the html
    li_tags = soup.find_all("li")
    for li in li_tags:
        if li.a and li.a.string:
            tag_list.append(li.a.string)
        if li.string:
            tag_list.append(li.string)

    # delete duplicates from the list of potential hospitals
    tag_list_no_duplicates = list(dict.fromkeys(tag_list))

    # list of common words that appear in hospital names (counted using Counter)
    common_words = ["Hospital", "Centre", "Health", "General", "Regional", "Community", "District", "Memorial", "Clinic", "Institute", "Cancer", "Hôpital", "Hôtel-Dieu"]

    # filter by common words to detect hospitals in the list
    hospital_list = [hospital for hospital in tag_list_no_duplicates if any(word in hospital for word in common_words)]

    geocoded_hospitals = get_hospital_geocodes(hospital_list)

    with open("hospitals.json", "w") as hospitals_json:
        hospitals_json.write(json.dumps(geocoded_hospitals, indent=2))

if __name__ == '__main__':
    main()