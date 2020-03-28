"""
This file will be run by GCloud functions every hour to update the data.
Make sure it runs smoothly and reports any errors clearly.
"""

import script


def main(event, context):
    print("Getting data from spreadsheet...")

    data = script.get_spreadsheet_data()

    print("Preprocessing data...")

    confirmed_cases_df, last_updated = script.preprocess_confirmed_cases_sheet(data)

    print("Geocoding data...")

    output = script.geocode_sheet(confirmed_cases_df, last_updated)

    print("Outputting data to file...")
    script.output_json(output)
    print("Done")


if __name__ == '__main__':
    main(None, None)
