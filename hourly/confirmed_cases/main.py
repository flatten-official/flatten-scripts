"""
This file will be run by GCloud functions every hour to update the data.
Make sure it runs smoothly and reports any errors clearly.
"""

import script
import time


def main(event, context):
    print("Getting data from spreadsheet...")

    data = script.get_spreadsheet_data()

    print("Geocoding data...")

    output = script.geocode_sheet(data)

    print("Outputting data to file...")
    script.output_json(output)
    print("Done")


if __name__ == '__main__':
    main(None, None)
