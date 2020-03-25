# Flatten-Scripts
The scripts that parse, format and run the data behind the scenes of [flatten.ca/heat-map](flatten.ca/heat-map).

## Repo Structure

`\hourly` folder contains scripts for hourly updating data, including confirmed cases, potential cases and vulnerable populations. 

Both the later two numbers are extracted from flatten.ca forms and are mapped by 3-digit postal code area.

`\others` folder contains scripts for static data, including postal code boundaries and random form samples.

## Important Guidelines
- All contributions should be made on a new branch and a pull request to *master* should be made.

- Use the Issues tabs to keep track of bugs, improvements and more. Use the Projects tab to keep track of work!

## Prerequisites
[Python 3.5+](https://www.python.org/)

## Setting up

- Install Google Cloud SDK and in the `[default]` confircuration login with your <name>@flatten.ca account. Tutorial: https://cloud.google.com/sdk/docs/quickstarts.
            
- 

- Install the following Python libraries: `pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib google-cloud-storage google-cloud-datastore`

## Running a script
Run the script you want to with Python 3. In the hourly folders, always run `main.py`.


## Contact
Martin, Will, Rupert, Ivan, Charlie

## Internal Notes

### Format for form data

**`form_data.js`**

```
{
            "total_responses" : 6969, # total number of reports recieved
            "max" : 9992,
            "time" : 29483929829, # UTC unix timestamp in ms since the origin
            "fsa" : {
                "B1A" : {"number_reports": 4938, "pot": 23, "risk": 18},
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


### Setting up Firebase Storage for the first time

This only needs to be done once per project, so don't worry about it.

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


### Deploying on Cloud Build

Everything should work more or less out of the box, apart from the fact that you have to set the `_BRANCH` envoronment variable to `prod` for prouduction or `dev` for development.


### Deploying the cloud functions

You will need to set the appropriate environment variables for each.

For form_data_generator, these are
* GCS_BUCKET, should point to that which to upload the data
* UPLOAD_FILE, the name of the file to upload the confirmed cases data to
* DS_NAMEPSACE, datastore namespaace to get the data from
* DS_KIND, the datastore kind to load data from

For `confirmed_cases`, they are:
* SPREADSHEET_ID, should (at the moment) be 
* GCS_BUCKET, should point to that which to upload the data
* UPLOAD_FILE, the name of the file to upload the confirmed cases data to
* SHEETS_API_KEY, API key for google sheets.

## Credits

Thank you to Statistics Canada for the following data.

Census Forward Sortation Area Boundary File, 2016 Census._ Statistics Canada Catalogue no. 92-179-X.

## 
