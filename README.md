# Flatten-Scripts

Backend scripts for the FLATTEN project.

## Project Structure

Each folder in this repository represents a microservice on google cloud run. The root directory contains configuration common to all projects.

The project is structured as follows:

```
src/
    main.py # runs the flask server that gets triggered by pubsub
    project1/
        main.py # contains the main() function which is picked up by the overall main.py and run by the flask server.
        other_import.py # contains the project specific code
    project2/
        main.py
requirements.txt # contains the project requirements
```

NB WIP DOCS
remember to include a main template

## Setup

Some steps in this section taken from the following two guides:
* [Continuous deployment with cloud build](https://cloud.google.com/run/docs/continuous-deployment-with-cloud-build)
* [Triggering from pub sub push](https://cloud.google.com/run/docs/triggering/pubsub-push#create-push-subscription)


### Manual Deployment for Testing

:warning: Make sure you have your project set to the staging before proceeding. :warning:

`gcloud config set project flatten-staging-271921`

Then, to get application default credentials

`gcloud auth login`

To deploy the docker image from your current `HEAD` to test:

`gcloud builds submit --substitutions COMMIT_SHA=manual`


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

gcloud beta pubsub subscriptions create cloud-run-scripts-pubsub --topic flatten-scripts \
   --push-endpoint=https://flatten-scripts-6eoeawk53a-ue.a.run.app/ \
   --push-auth-service-account=run-scripts-pubsub@flatten-staging-271921.iam.gserviceaccount.com
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