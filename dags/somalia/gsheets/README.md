# Somalia GSheet Upload

This script uploads the data from the Google Datastore for Somalia to a Google Sheets.

## `columns.txt`

This file specifies the columns in the Google sheet.

- DO NOT change the order of the columns since this would break the Google Data Studio

- Always add new columns to the end of the file

- DO NOT remove outdated columns since this will break the Google Sheet.

- Ensure the column name matches the column custom pre-fill key from paperform

- `timestamp`, the first column name is an exception and is not actually a field in the paperform. The Python code accounts for this, do not remove this column.

## `excluded_columns.txt`

A list of columns (paperform pre-fill custom keys) that should not be added to the GSheet. Order is unimportant. Do not include values that use to be in `columns.txt`.

## Code logic

1. Create a query to the Google DataStore

2. Execute the query and loop through results

3. For each row in the database add the row to a list of rows.

4. Get the credentials required to upload to Google Sheets from the GCP Secret Manager

5. Upload to the Google Sheets the list of rows (now that we have the credentials).

Notes: 

- The Google Sheets is overwritten on every run.

- The whole database is read on every run.

## First time setup

1. Create a GSheet with the correct tabs (tab names match `TAB_NAMES`).

2. Create a service account and share the sheet with the service account.

3. Upload the service account's credentials to the GCP Secret Manager.