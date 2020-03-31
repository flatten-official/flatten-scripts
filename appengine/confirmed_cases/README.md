<<<<<<< HEAD
# Extra details for confirmed cases script
=======
# Confirmed Cases Script

## Testing Locally

Set the environment variables (`export ENV_VAR=VALUE`) used in the script.
Ensure you have the requirements installed, and run `main.py`

You should just be able to steal the values you want from the `app.staging.yaml` file.
>>>>>>> prov-data-new

Note that this currently contains our google sheets API key - we are aware this is somewhat insecure,
however we have not yet implemented a permanent fix (although it doesn't actually grant access to anything,
so it is not a huge issue for now; this solution is tempoary - we are intending on moving to the Cloud Key Manager when we have time to
<<<<<<< HEAD
write this!)
=======
write this!)

## Deploying to App Engine (for testing)

:warning: make sure you are deploying on the correct project - you want to set your project to staging - `gcloud config set project flatten-staging-271921` `:warning:

`cd` to this directory, and run `gcloud app deploy .`

## Deploy for Staging / Production

Nothing to be done here - just get your PR merged to the `staging` or `master` branches.
>>>>>>> prov-data-new
