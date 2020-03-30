# App engine information

Each folder in this directory is a service to be deployed on Google App Engine (GAE).

Instructions pertain to each service as they are fully independent.

## Testing Locally

1. Set the required environment variables if any. Check `app.staging.yaml` for which variables are required and their values.
On 'nix, to set environment variables run `export ENV_VAR=VALUE`. Set `DEBUG` to `'True'` to help with development locally.

2. Install the required libraries.
   `pip install -r requirements.txt`
   
3. Run `gcloud auth application-default login` to generate the credentials for th Python scripts. Requires GCloud Admin SDK.

3. Run `main.py`. This will launch the server on port 8080.

4. Ping the server to get the script started. Ensure to set the `X-Appengine-Cron` header to `true`. This is how we ensure no external requests are accepted on GAE (GAE doesn't allow any such requests through the firewall).

`curl -i -H "X-Appengine-Cron: true" localhost:8080`

## Deploying to App Engine (for testing)

:warning: make sure you are deploying on the correct project - you want to set your project to staging - `gcloud config set project flatten-staging-271921` `:warning:

`cd` to service's directory, and run `gcloud app deploy .`

Note that you cannot directly ping these services on App engine, due to the aforementioned cron header, which will get stripped if you try to manually ping externally.

## Deploying App Engine Services with Cloud Build

Everything should work more or less out of the box for a triggered deploy from a merge.
If you want to test a manual deployment on the staging instance, you have to set the `BRANCH_NAME` variable substitution to `staging` 

Run in the root directory: 

`gcloud builds submit --substitutions BRANCH_NAME=staging`

## Deploy for Staging / Production

Nothing to be done here - just get your PR merged to the `staging` or `master` branches.