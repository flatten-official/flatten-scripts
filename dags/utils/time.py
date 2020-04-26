from pytz import utc
import datetime

def get_string_date(timestamp):
    # timestamp is in ms since UNIX origin, so divide by 1000 to get seconds
    ts_sec = timestamp / 1000
    # make a UTC datetime object from the timestamp, convert to a day stamp
    day = utc.localize(
        datetime.datetime.utcfromtimestamp(ts_sec)
    ).strftime('%Y-%m-%d')
    return day
