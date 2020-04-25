# Flatten Dags

This folder contains the DAG files and associated folders containing the code to run our ETL pipelines on cloud composer (a managed apache airflow service on Google Cloud.)

## Syncing DAGS

DAGS in this folder will be automatically synced to the appropriate places upon merge to master or staging. If you want to deploy from your local machine for testing, run


```gsutil -m rsync -d -r dags gs://us-central1-run-scripts-f78dc852-bucket/dags```

from the root of the repository.


## Updating PyPI packages.

If you require a new PyPi package to be installed on Cloud Composer, you should put a `requirements.txt` file in the associated folder.

You can sync this to the staging instance with `gcloud composer environments update run-scripts --update-pypi-packages-from-file requirements.txt --location us-central1`.

When merging to master, note to Arthur or Ivan that the requirements need to be updated, and tell them which packages to install.


## Testing Locally

Put an `if __name__` block in the file. We currently have not fully set up local airflow testing.


## Data Files

Data files need to be uploaded to the cloud composer `data` folder. This is mounted on each worker at `/home/airflow/gcs/data`.

You can upload these files to the staging bucket [here](https://console.cloud.google.com/storage/browser/us-central1-run-scripts-f78dc852-bucket/data?project=flatten-staging-271921&ref=https://console.cloud.google.com/storage/browser/us-central1-run-scripts-f78dc852-bucket/data?project%3Dflatten-staging-271921&rapt=AEjHL4N4_yz7VwqBT9_7p15VNp9iHN3Y6yOQY9kqDihm0TTWOMoOR0ZzQLjEiDd2fNz269Z6hoOrRwBxxA2IB-Eh8BOz8_Aedg)
for testing. Ask Arthur or Ivan to upload them to the master bucket for you before merging to master.

## Making New Dags

Each dag is associated with a `<NAME>_dag.py` folder in this directory. See current dags for examples. To make a new dag,
copy current examples here. You should put the associated code in a package in this folder of the same name as the dag.
This package must contain an `__init__.py` to be recognised as such.