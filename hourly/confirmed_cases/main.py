"""
This file will be run by GCloud functions every hour to update the data.
Make sure it runs smoothly and reports any errors clearly.
"""

import script


def main():
    print("Hello world")

    script.get_spreadsheet_data()

    for i in range(4):
        try:
            script.geocode_sheet()
        except:
            continue

    script.geocode_sheet()
    script.output_json()


if __name__ == '__main__':
    main()
