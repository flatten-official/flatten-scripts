"""
This file will be run by GCloud functions every hour to update the data.
Make sure it runs smoothly and reports any errors clearly.
"""

import script

def main(event, context):
    print("Getting data from spreadsheet...")

    confirmed, recovered, dead = script.get_spreadsheet_data()

    print("Geocoding data...")

    confirmed_output = script.geocode_sheet(confirmed)
    travel_data = script.get_travel_data(confirmed)
    provincial_data = script.get_provincial_totals(confirmed_output, recovered, dead)
    print("Outputting data to file...")
    script.output_json(confirmed_output, travel_data, provincial_data)
    print("Done")


if __name__ == '__main__':
    main(None, None)
