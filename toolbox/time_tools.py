import datetime
import time


def local_timestamp_now(tz_shift):
    """
    Returns current local time in unix style in seconds
    :param tz_shift: timezone shift in seconds in relation to UTC time
    :rtype: int
    """
    return int(time.time() + tz_shift)


def ts_to_datetime(timestamp):
    """
    Converts unix timestamp to datetime
    :param timestamp: unix timestamp
    :return: Datetime object
    """
    return datetime.datetime.utcfromtimestamp(timestamp)


def today_start_local_ts(curr_local_ts, tz_shift):
    """
    Local unix timestamp equal to current day 00:00 local time
    :param curr_local_ts: current timestamp in local timezone
    :param tz_shift: timezone shift in seconds
    :return: timestamp of local day start
    :rtype: int
    """
    curr_local_datetime = ts_to_datetime(curr_local_ts)
    day_start_local_datetime = curr_local_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_local_timestamp = day_start_local_datetime.timestamp() + tz_shift
    return int(day_start_local_timestamp)


def today_end_local_ts(tz_shift):
    """
    Local unix timestamp equal to next day 00:00 local time
    :param tz_shift: timezone shift in seconds
    :return: timestamp of local day end
    :rtype: int
    """
    curr_local_ts = local_timestamp_now(tz_shift)
    curr_local_datetime = ts_to_datetime(curr_local_ts)
    day_start_local_datetime = curr_local_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local_timestamp = day_start_local_datetime.timestamp() + tz_shift + 60*60*24
    return int(day_end_local_timestamp)


def prev_n_day_end_local(tz_shift, day_number=-1):
    """
    Local unix timestamp equal to nth day 00:00 local time
    :param tz_shift: timezone shift in seconds
    :param day_number: number of day in relation to current day=0
    :return: timestamp of local nth day start
    :rtype: int
    """
    curr_local_ts = local_timestamp_now(tz_shift)
    prev_day_end_ts = today_start_local_ts(curr_local_ts, tz_shift) - 1
    prev_n_day_end_ts = prev_day_end_ts + 60*60*24*(day_number + 1)
    return prev_n_day_end_ts
