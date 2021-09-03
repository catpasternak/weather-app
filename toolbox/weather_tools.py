from concurrent.futures import ThreadPoolExecutor
from functools import partial

import requests

from .time_tools import *

WEATHER_API_KEY = "631f57b7539b1908d2fb62f79486fd95"

URL_CURRENT = "https://api.openweathermap.org/data/2.5/weather"
URL_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"
URL_HISTORIC = "https://api.openweathermap.org/data/2.5/onecall/timemachine"


def get_city_timezone(latitude, longitude, url=URL_CURRENT, api_key=WEATHER_API_KEY):
    """
    Fetches city timezone from OpenWeatherMap API
    :param latitude: city latitude
    :param longitude: city longitude
    :param url: base url for API call
    :param api_key: API key provided by OpenWeatherMap
    :return: timezone (i.e. time shift in seconds from UTC time)
    :rtype: int
    """
    params = {'appid': api_key, 'lat': latitude, 'lon': longitude}
    resp = requests.get(url, params=params)
    return resp.json()['timezone']


def get_day_hist_temp(day_num, latitude, longitude, url=URL_HISTORIC, api_key=WEATHER_API_KEY):
    """
    Gets one day temperatures list. Days possible range: from 5 days ago till current moment.
    Today temperatures list is provided for part of the day that has passed.
    :param day_num: day number in range (-5, 0)
    :param latitude: city latitude
    :param longitude: city longitude
    :param url: base url for API calls for 5 days historic info
    :param api_key: API key provided by OpenWeatherMap
    :return: list of day temperatures fixed every hour (24 values per day except today that is less)
    :rtype: List[float]
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


def get_all_hist_temp(coords_tuple, threads=4):
    """
    Gets historic day temperature lists for all days in range: from 5 days ago till current moment.
    One list per day. Today temperatures list is provided for part of the day that has passed.
    :param coords_tuple: latitude and longitude
    :type coords_tuple: tuple[float]
    :param threads: number of threads for parallel request to weather service API
    :return: Lists of day temperatures for last 5 days plus part of today
    :rtype: Generator[list[float]]
    """
    lat, lon = coords_tuple
    get_city_day_temp = partial(get_day_hist_temp, latitude=lat, longitude=lon)
    days = (day for day in range(-5, 1))
    with ThreadPoolExecutor(max_workers=threads) as pool:
        results = pool.map(get_city_day_temp, days)
    return results


def get_forecast_temp_list(coord_tuple):
    """
    Gets today and 4 coming days temperature forecast
    :param coord_tuple: latitude and longitude
    :return: tuple of 2 elements: list of today forecast temperatures and list of lists of coming 4 days temps
    :rtype: tuple[list[list],list[list[list]]]
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
