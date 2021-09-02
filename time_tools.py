import datetime
import time


def local_timestamp_now(tz_shift):
    return int(time.time() + tz_shift)


def ts_to_datetime(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp)


def today_start_local_ts(curr_local_ts, tz_shift):  # check
    curr_local_datetime = ts_to_datetime(curr_local_ts)
    day_start_local_datetime = curr_local_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_local_timestamp = day_start_local_datetime.timestamp() + tz_shift
    return int(day_start_local_timestamp)


def today_end_local_ts(tz_shift):  # check
    curr_local_ts = local_timestamp_now(tz_shift)
    curr_local_datetime = ts_to_datetime(curr_local_ts)
    day_start_local_datetime = curr_local_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local_timestamp = day_start_local_datetime.timestamp() + tz_shift + 60*60*24
    return int(day_end_local_timestamp)


def prev_n_day_end_local(tz_shift, day_number=-1):
    curr_local_ts = local_timestamp_now(tz_shift)
    prev_day_end_ts = today_start_local_ts(curr_local_ts, tz_shift) - 1
    prev_n_day_end_ts = prev_day_end_ts + 60*60*24*(day_number + 1)
    return prev_n_day_end_ts
