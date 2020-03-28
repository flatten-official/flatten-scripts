# Confirmed Cases Script

## Testing Locally

Set the environment variables (on 'nix, `export ENV_VAR=VALUE`) used in the script.
Ensure you have the requirements installed, and run `main.py`

You should just be able to steal the values you want from the `app.staging.yaml` file.

When you ping it locally, you must set the `X-Appengine-Cron` header to `true`, for example, like `curl -i -H "X-Appengine-Cron: true" localhost:8080`.
This is used for security on GAE, to ensure that the service is being pinged by an App Engine cron job.

## Deploying to App Engine (for testing)

:warning: make sure you are deploying on the correct project - you want to set your project to staging - `gcloud config set project flatten-staging-271921` `:warning:

`cd` to this directory, and run `gcloud app deploy .`

Note that you cannot directly ping these services on App engine, due to the aforementioned cron header, which will get stripped if you try to manually ping externally.

## Deploy for Staging / Production

Nothing to be done here - just get your PR merged to the `staging` or `master` branches.