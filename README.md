# Flatten-Scripts
The scripts that parse, format and run the data behind the scenes of [flatten.ca/heat-map](flatten.ca/heat-map).

## Important Guidelines
- All contributions should be made on a new branch and a pull request to *master* should be made.

- Use the Issues tabs to keep track of bugs, improvements and more. Use the Projects tab to keep track of work!

## Prerequisites
[Python 3.5+](https://www.python.org/)

## Setting up

- Install Google Cloud SDK and in the `[default]` configuration login with your name@flatten.ca account. Tutorial: https://cloud.google.com/sdk/docs/quickstarts.

- Run `gcloud auth application-default login` to generate the credentials for th Python scripts.
            
- run `pip install -r requirements.txt` in both `appengine/confirmed_cases` and `appingine/form_data_generator` to install dependencies.

## App Engine

`appengine/` contains the Google App engine services. See their respective READMEs. Note this also contains the `cloudbuild.yaml` that builds it for cloud run.


### Deploying App Engine Services on Cloud Build

Everything should work more or less out of the box for a triggered deploy from a merge.
If you want to test a manual deployment on the staging instance, you have to set the `BRANCH_NAME` variable substitution to `staging` 

`gcloud builds submit --substitutions BRANCH_NAME=staging`

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

## Credits

Thank you to Statistics Canada for the following data.

Census Forward Sortation Area Boundary File, 2016 Census._ Statistics Canada Catalogue no. 92-179-X.

## 
