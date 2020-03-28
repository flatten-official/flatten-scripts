# Flatten-Scripts

Backend scripts for the FLATTEN project.

## Contact
Martin, Will, Rupert, Ivan, Charlie, Arthur

## Project Structure

Each folder in this repository represents a microservice. These run on a unified google cloud run container.

The root directory contains configuration common to all projects.

The project is structured as follows:

```
src/
    main.py # runs the flask server that gets triggered by pubsub
    project1/
        script.py # contains the main() function which is picked up by the overall main.py and run by the flask server.
        other_import.py # contains the project specific code. can import from main.py as import other_import.py
    project2/
        script.py
requirements.txt # contains the project requirements. please include requirements for each script
```

Each project directory contains a `script.py` file with a `main` function that is called when your service is triggered.
The `main` function is passed the message attributes as a dictionary. See pre-existing scripts for examples.

## Important Guidelines
- All contributions should be made on a new branch and a pull request to *master* should be made.

- Use the Issues tabs to keep track of bugs, improvements and more. Use the Projects tab to keep track of work!

## Prerequisites
[Python 3.6+](https://www.python.org/)

## Setting up

- Install the Google Cloud SDK, and authorise.

- Install the dependencies `pip install -r requirements.txt`

##Testing

### Local Testing

1. Make sure you have all dependencies installed.
2. Go into whichever directory the `script.py` you want to run is located in.
3. Take out the `def main(message_attributes):` line and remove the indent of the code below it.
4. Run `gcloud auth application-default login` and make sure the account you use has a `storage admin` role.
5. Run `python3 script.py`.

### Manual Deployment for Testing

:warning: Make sure you have your project set to the staging before proceeding. :warning:

`gcloud config set project flatten-staging-271921`

Then, to get application default credentials

`gcloud auth login`

To deploy the docker image from your current `HEAD` to test:

`gcloud builds submit --substitutions COMMIT_SHA=manual`

You can ping the requisite service with

`gcloud pubsub topics publish <name_of_cloud_service> --message "hello"`

You can also add attributes, which will be passed by the flask server to your `main()` as a dictionary.

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


## Setup of Cloud Run

Some steps in this section taken from the following two guides:
* [Continuous deployment with cloud build](https://cloud.google.com/run/docs/continuous-deployment-with-cloud-build)
* [Triggering from pub sub push](https://cloud.google.com/run/docs/triggering/pubsub-push#create-push-subscription)



### Pub Sub

Create the service account to be used for pub sub: `gcloud iam service-accounts create SERVICE-ACCOUNT_NAME \
   --display-name "DISPLAYED-SERVICE-ACCOUNT_NAME"`

Give the service account the permission to invoke your service
```
gcloud run services add-iam-policy-binding SERVICE \
   --member=serviceAccount:SERVICE-ACCOUNT_NAME@PROJECT-ID.iam.gserviceaccount.com \
   --role=roles/run.invoker
```

Set up the pub sub subscription with 
Create a pub sub topic: `gcloud pubsub topics create TOPIC-NAME`

Subscribe the service to recieve messages from the topic

`gcloud projects add-iam-policy-binding flatten-staging-271921 --member=serviceAccount:service-233853318753@gcp-sa-pubsub.iam.gserviceaccount.com --role=roles/iam.serviceAccountTokenCreator`

```
gcloud beta pubsub subscriptions create SUBSCRIPTION-ID --topic TOPIC-NAME \
   --push-endpoint=SERVICE-URL/ \
   --push-auth-service-account=SERVICE-ACCOUNT-NAME@PROJECT-ID.iam.gserviceaccount.com
```
Where you can obtain service-url using
`gcloud run services describe SERVICE-NAME`


`
gcloud projects add-iam-policy-binding PROJECT-ID \
     --member=serviceAccount:service-PROJECT-NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com \
     --role=roles/iam.serviceAccountTokenCreator
`

Test with
`gcloud pubsub topics publish TOPIC --message "hello"`


### Environment Variables

You can set and update environment variables as described [here](https://cloud.google.com/run/docs/configuring/environment-variables).

You will need to set the appropriate environment variables for each.

For form_data_generator, these are
* GCS_BUCKET_FORM, should point to that which to upload the data
* UPLOAD_FILE_FORM, the name of the file to upload the confirmed cases data to
* DS_NAMEPSACE, datastore namespaace to get the data from
* DS_KIND, the datastore kind to load data from

For `confirmed_cases`, they are:
* SPREADSHEET_ID, should (at the moment) be 
* GCS_BUCKET_CONFIRMED, should point to that which to upload the data
* UPLOAD_FILE_CONFIRMED, the name of the file to upload the confirmed cases data to
* SHEETS_API_KEY, API key for google sheets.

=======
The scripts that parse, format and run the data behind the scenes of [flatten.ca/heat-map](flatten.ca/heat-map).

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
