from pytz import utc
from datetime import datetime, timezone


def get_string_date(timestamp):
    # timestamp is in ms since UNIX origin, so divide by 1000 to get seconds
    # make a UTC datetime object from the timestamp, convert to a day stamp
    return utc.localize(datetime.utcfromtimestamp(timestamp / 1000)).strftime('%Y-%m-%d')


def get_printable_cur_time():
    return datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%d, %H:%M:%S    (UTC)")
