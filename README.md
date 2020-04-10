# Flatten-Scripts
The scripts that parse, format and run the data behind the scenes of [flatten.ca/heat-map](flatten.ca/heat-map).

## Important Guidelines
- All contributions should be made on a new branch and a pull request to *staging* should be made.

- Use the Issues tabs to keep track of bugs, improvements and more. Use the Projects tab to keep track of work!

## Prerequisites
[Python 3.5+](https://www.python.org/)

## Setting up app engine scripts

- Look at the README in `\appengine` for setting up the app engine scripts.

Note this directory contains the `cloudbuild.yaml` that builds the GAE services.

Make sure the Cloud Build script has the permissions required to deploy a cron job with

`gcloud projects add-iam-policy-binding $(PROJECT_ID) --member=serviceAccount:$(PROJECT_NUMBER)@cloudbuild.gserviceaccount.com --role=roles/cloudscheduler.admin`

## Contact
Martin, Arthur, Will, Rupert, Ivan, Charlie

## Internal Notes

### Format for form data

**`form_data.js`**

```
{
            "total_responses" : 6969, # total number of reports recieved
            "max" : 9992,
            "time" : 29483929829, # UTC unix timestamp in ms since the origin
            "fsa" : {
                "B1A" : {"number_reports": 4938,
                "pot": 23, "risk": 18, "both": 2, # NB these are not included if fsa excluded is true
                "fsa_excluded": false # flag to include / exclude regions with less than 50 people from census data
            },
                .
                .
                .
    }
} 
```


**`form_data_usa.json`**
```
{
            "total_responses" : 6969, # total number of reports recieved
            "max" : 9992,
            "time" : 29483929829, # UTC unix timestamp in ms since the origin
            "county" : {
                # key here is the county NSCODE
                "00198163" : {"number_reports": 4938,
                "pot": 23, "risk": 18, "both": 2, # NB these are not included if fsa excluded is true
                "county_excluded": false #  flag to exclude regions with too few people in them
            },
                .
                .
                .
    }
} 
```

`confirmed.json`

```
{
    "last_updated" : "Date accessed at: 23/03/2020...",
    "max_cases" : 230,
    "confirmed_cases": [
        {
            "name" : "Algoma, Ontario",
            "cases" : 1,
            "coord" : [44.289, -79.8536]
        },
        {
            "name" : "Bas-Saint-Laurent, Quebec",
            "cases" : 2,
            "coord" : [48.30, 23.4]
        }
    ]
}
```
`travel_data.json`
```
{
 "cases": 7448,
 "travel_cases": 420,
 "no_travel_cases": 219,
 "not_reported": 6809,
 "travel_data": {
  "Not Reported": 93,
  "United States": 79,
  "Iran": 26,
  "Egypt": 22,
  "Cruise": 21,
  .
  .
  .
  "Denmark": 1
 },
 "no_travel_data": {
  "Close Contact": 204,
  "Community": 15
 }
}
```
Note: countries are doubled counted since sometimes, one infected person travelled to multiple countries.

`provincial_data.json`
```
{
 "Quebec": {
  "cases": 3430,
  "recovered_cumul": {
   "30-03-2020": 84,
   "29-03-2020": 84
  },
  "dead_daily": {
   "30-03-2020": 3
  },
  "total_recovered": 84,
  "total_dead": 3
 }
}
```

`hospitals.json`
```
{
  "geocode_successes": [
    {
      "name": "Athabasca Healthcare Center",
      "location": [
        -113.25577819764706,
        54.71815025
      ]
    },
    {
      "name": "Banff Mineral Springs Hospital",
      "location": [
        -115.57643898349642,
        51.178997100000004
      ]
    }
  ],
  "geocode_failures": [
    "Cochrane Community Health Centre",
    "St Joseph's Auxiliary Hospital"
  ]
}
```
### Setting up Firebase Storage for the first time

This only needs to be done once per project (not per user), so don't worry about it.

To allow the frontend of the map to read from the cloud storage buckets (storing the data), you will need to set the origin policy to allow reading of the cloud storage buckets. Add the following to a file called cors.json:
```
    [
      {
    "origin": ["*"],
    "method": ["GET"],
    "maxAgeSeconds": 3600
  }
]
```
then run 
```gsutil cors set cors.json gs://flatten-staging-271921.appspot.com```

You need to ensure that the firebase rules on the bucket are set up to allow reading of the files externally.

## Credits

Thank you to Statistics Canada for the following data.

Census Forward Sortation Area Boundary File, 2016 Census._ Statistics Canada Catalogue no. 92-179-X.

