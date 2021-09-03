from concurrent.futures import ThreadPoolExecutor
from functools import partial

import requests

from .time_tools import *

WEATHER_API_KEY = "631f57b7539b1908d2fb62f79486fd95"

URL_CURRENT = "https://api.openweathermap.org/data/2.5/weather"
URL_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"
URL_HISTORIC = "https://api.openweathermap.org/data/2.5/onecall/timemachine"


def get_city_timezone(latitude, longitude, url=URL_CURRENT, api_key=WEATHER_API_KEY):
    params = {'appid': api_key, 'lat': latitude, 'lon': longitude}
    resp = requests.get(url, params=params)
    return resp.json()['timezone']


def get_day_hist_temp(day_num, latitude, longitude, url=URL_HISTORIC, api_key=WEATHER_API_KEY):
    """
    Gets any day back (from -5 to 0) 1 day temp list.
    """
    if not day_num:
        time_threshold = int(time.time() - 5)
    else:
        timezone = get_city_timezone(latitude, longitude)
        time_threshold_local = prev_n_day_end_local(timezone, day_number=day_num)
        time_threshold = time_threshold_local - timezone
    params = {'appid': api_key, 'lat': latitude, 'lon': longitude, 'dt': time_threshold, 'units': 'metric'}
    resp = requests.get(url, params=params)
    data = resp.json()
    day_temp_list = [record['temp'] for record in data['hourly']]
    return day_temp_list


def get_all_hist_temp(coords_tuple):
    lat, lon = coords_tuple
    get_city_day_temp = partial(get_day_hist_temp, latitude=lat, longitude=lon)
    days = (day for day in range(-5, 1))
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = pool.map(get_city_day_temp, days)
    return results


def get_forecast_temp_list(coord_tuple):
    """
    Gets tuple of 2 elements: list of today forecast temperatures and tuple of lists of coming 4 days data
    """
    latitude, longitude = coord_tuple
    tz_shift = get_city_timezone(latitude, longitude)
    threshold_ts = today_end_local_ts(tz_shift)
    url = URL_FORECAST
    api_key = WEATHER_API_KEY
    params = {'appid': api_key, 'lat': latitude, 'lon': longitude, 'units': 'metric'}
    resp = requests.get(url, params=params)
    data = resp.json()
    forecast_today = [record['main']['temp'] for record in data['list'] if record['dt'] < threshold_ts]
    forecast_4days_plus = [record['main']['temp'] for record in data['list'] if record['dt'] >= threshold_ts]
    forecast_4days = [forecast_4days_plus[start:stop] for start, stop in ((0, 8), (8, 16), (16, 24), (24, 32))]
    return forecast_today, forecast_4days
