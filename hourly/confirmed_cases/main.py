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

    output = None
    last_updated = None

    for i in range(4):
        try:
            output, last_updated = script.geocode_sheet(data)
        except:
            print("Attempt " + str(i + 1) + " failed.")
            time.sleep(2)

    output, last_updated = script.geocode_sheet(data)

    print("Outputting data to file...")
    script.output_json(output, last_updated)
    print("Done")


if __name__ == '__main__':
    main(None, None)
