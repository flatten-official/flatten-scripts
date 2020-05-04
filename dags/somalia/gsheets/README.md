# Somalia GSheet Upload

This script uploads the data from the Google Datastore for Somalia to a Google Sheets.

## First time setup

1. Create a GSheet with the correct tabs (tab names match `TAB_NAMES`).

2. Create a service account and share the sheet with the service account.

3. Upload the service account's credentials to the GCP Secret Manager.

## Adding a column after adding a question to the paperform.

Simply add the paperform question's custom pre-fill key to `columns.txt` AT THE END OF THE FILE. DO NOT insert the 
pre-fill key in the middle of the list and DO NOT change the order of other pre-fill keys.

## Code details

### Code steps

1. Create a query to the GCP DataStore

2. Execute the query and loop through results

3. For each row in the database add the row to a list of rows. This forms the first tab.

4. Format the data (e.g. replacing None with an empty string)

5. Generate the Info tab.

6. Get the credentials required to upload to Google Sheets from the GCP Secret Manager.

7. For each tab, clear the tab and upload the new tab content to the GSheets.

### `columns.txt`

This file specifies the columns in the Google sheet.

- DO NOT change the order of the columns since this would break the Google Data Studio

- Always add new columns to the end of the file

- DO NOT remove outdated columns since this will break the Google Sheet.

- Ensure the column name matches the column custom pre-fill key from paperform

- `timestamp`, the first column name is an exception and is not actually a field in the paperform. The Python code accounts for this, do not remove this column.

### `excluded_columns.txt`

A list of columns (paperform pre-fill custom keys) that should not be added to the GSheet. Order is unimportant. Do not include values that use to be in `columns.txt`.


### Notes 

- The Google Sheets is erased and re-written on every run. Hence, the whole database is read on every run. 
This ensures that the script is not dependant on it's outputs (the GSheet). Having the one-way stream avoids issues.

### Documentation of data structure of the data column in an entity of the datastore
```
data: {
    "death_age": {          # Matches custom_key
      "entityValue": {
        "properties": {
          "type": {
            "stringValue": "number"
          },
          "value": {
            "nullValue": null
          },
          "custom_key": {
            "stringValue": "death_age"
          },
          "title": {
            "stringValue": "Da’da qofka dhintay"
          },
          "key": {
            "stringValue": "6aqpk"
          },
          "description": {
            "nullValue": null
          }
        }
      }
    },
    .
    .
    .
  }
  ```