# Confirmed Cases Script

## Testing Locally

Set the environment variables (`export ENV_VAR=VALUE`) used in the script.
Ensure you have the requirements installed, and run `main.py`

You should just be able to steal the values you want from the `app.staging.yaml` file.


## Deploying to App Engine (for testing)

:warning: make sure you are deploying on the correct project - you want to set your project to staging - `gcloud config set project flatten-staging-271921` `:warning:

`cd` to this directory, and run `gcloud app deploy .`

## Deploy for Staging / Production

Nothing to be done here - just get your PR merged to the `staging` or `master` branches.