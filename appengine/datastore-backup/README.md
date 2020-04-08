# flatten-data-export
Export data from datastore into a cloud storage bucket


## Setting up

* Set up the IAM policies as detailed in [the docs](https://cloud.google.com/datastore/docs/schedule-export)

* Also ensure that you give the Cloud Build service account the cloud scheduler admin role, as this is needed to deploy the cron jobs:
  - `projects add-iam-policy-binding $(PROJECT_ID) --member=serviceAccount:$(PROJECT_NUMBER)@cloudbuild.gserviceaccount.com --role=roles/cloudscheduler.admin`
