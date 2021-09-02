import os
import json
import time
from zipfile import ZipFile
import requests
from iso3166 import countries_by_alpha2
from sqlalchemy.sql import text
from geopy.geocoders import Nominatim, OpenMapQuest
from geopy.extra.rate_limiter import RateLimiter
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from collections import defaultdict
from time_tools import *


GEOCODE_API_KEY = 's23N9lets5Gey28fkbpt3ub8v4N6efyk'

WEATHER_API_KEY = "631f57b7539b1908d2fb62f79486fd95"
URL_CURRENT = "https://api.openweathermap.org/data/2.5/weather"
URL_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"
URL_HISTORIC = "https://api.openweathermap.org/data/2.5/onecall/timemachine"


def combine_path(*path_parts: str):
    return os.path.join(os.path.dirname(__file__), *path_parts)


# Unzip ###################


def unzip_next_to(file_path):
    new_dir = os.path.join(os.path.dirname(file_path), 'hotels')
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
    with ZipFile(file_path, 'r') as zipObj:
        if not os.listdir(new_dir):
            zipObj.extractall(new_dir)
    return new_dir


# Data validation #################


def is_country(country):
    if country in countries_by_alpha2:
        return True
    return False

def is_coordinate(string):
    try:
        value = float(string)
    except ValueError:
        return False
    if abs(value) > 180:
        return False
    return True


# Read files and fill DB ############################


def yield_valid_records(input_file):
    _ = input_file.readline()
    while line := input_file.readline():
        fields = line.strip().split(',')
        if len(fields) != 6:
            continue
        if not all((fields[1], is_country(fields[2]), fields[3], is_coordinate(fields[4]), is_coordinate(fields[5]))):
            continue
        yield str(fields[1]), fields[2], fields[3], float(fields[4]), float(fields[5])


def read_validated(file_path, tokenize=yield_valid_records):
    with open(file_path) as input_file:
        yield from tokenize(input_file)


def add_records_to_table(file_path, session, cls):
    for record in read_validated(file_path):
        hotel = cls(name=record[0], country=record[1], city=record[2], latitude=record[3], longitude=record[4])
        session.add(hotel)


def fill_table_from_csv(file_dir, session, cls):
    if session.query(cls).first():
        return False
    for file in os.listdir(file_dir):
        if file.endswith('.csv'):
            add_records_to_table(combine_path(file_dir, file), session, cls)
    session.commit()


######### Find major cities #######################################


def find_cities(session, cls):
    stmt = text(
        "select h.COU1, h.C1 from (select country as COU1, city as C1, count(name) as I from hotels group by COU1, C1) as h inner join (select h3.COU3 as COU4, max(h3.K) as m from (select country as COU3, count(name) as K from hotels group by COU3, city) as h3 group by h3.COU3) as h4 on (h4.COU4 = h.COU1 and h4.m = h.I)"
                ).columns(cls.country, cls.city)
    query = session.query(cls).from_statement(stmt).all()
    return {obj.country: obj.city for obj in query}


############## Fill addresses for major cities hotels ########################################


def get_address(latitude, longitude, api_key=GEOCODE_API_KEY):
    # geolocator = Nominatim(user_agent=user_agent)
    geolocator = OpenMapQuest(api_key=api_key)
    geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1/20, max_retries=3)
    location = geocode(f'{latitude}, {longitude}', timeout=1)
    return location.address


def attach_address_if_in_major_city(hotel, major_cities):
    # major_cities = {'AT': 'Vienna', 'ES': 'Barcelona', 'FR': 'Paris', 'GB': 'London', 'IT': 'Milan', 'NL': 'Amsterdam', 'US': 'Houston'}
    if hotel.city == major_cities.get(hotel.country) and not hotel.address:
        lat, lon = hotel.latitude, hotel.longitude
        address = get_address(lat, lon)
        hotel.address = address
    return hotel


def fill_addresses_for_major_cities(session, cls, major_cities, threads=4):
    s = time.time()
    print('started main fill addresses function')
    attach_address = partial(attach_address_if_in_major_city, major_cities=major_cities)
    hotels = session.query(cls).all()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        pool.map(attach_address, hotels)
    session.commit()
    print(time.time() - s, 'seconds took geocoding execution with', threads, 'threads')

#################  ###########################################


def get_hotels_coordinates(session, cls, country, city):
    coordinates_list = []
    query = session.query(cls).filter(cls.country == country, cls.city == city).all()
    for hotel in query:
        coordinates_list.append((float(hotel.latitude), float(hotel.longitude)))
    return coordinates_list


def find_city_center(coordinates_list):
    lat_sum = lon_sum = 0
    count = 0
    for latitude, longitude in coordinates_list:
        lat_sum += latitude
        lon_sum += longitude
        count += 1
    avg_lat, avg_lon = lat_sum/count, lon_sum/count
    return avg_lat, avg_lon


def get_major_cities_coordinates(session, cls, major_cities):
    coordinates = defaultdict()
    for country, city in major_cities.items():
        coordinates_list = get_hotels_coordinates(session, cls, country, city)
        coordinates[(country, city)] = find_city_center(coordinates_list)
    return coordinates


def fill_table_with_coordinates(session, source_cls, target_cls, major_cities):
    city_centers = get_major_cities_coordinates(session, source_cls, major_cities)
    if not session.query(target_cls).first():
        for city, coords in city_centers.items():
            session.add(target_cls(country=city[0], city=city[1], latitude=coords[0], longitude=coords[1]))
        session.commit()


########## Temperature API calls ######################################################################################


def get_city_timezone(latitude, longitude, url=URL_CURRENT, api_key=WEATHER_API_KEY):
    params = {'appid': api_key, 'lat': latitude, 'lon': longitude}
    resp = requests.get(url, params=params)
    return resp.json()['timezone']


def get_day_hist_temp(latitude, longitude, day_num=-1, url=URL_HISTORIC, api_key=WEATHER_API_KEY):
    """
    Gets any day back (from -5 to 0) 1 day temp list.
    """
    if not day_num:
        time_threshold = int(time.time() - 5)
    else:
        timezone = get_city_timezone(latitude, longitude)
        time_threshold_local = prev_n_day_end_local(timezone, day_number=day_num)
        time_threshold = time_threshold_local - timezone
    # print(latitude, longitude)
    # print(time_threshold)
    params = {'appid': api_key, 'lat': latitude, 'lon': longitude, 'dt': time_threshold, 'units': 'metric'}
    resp = requests.get(url, params=params)
    data = resp.json()
    # print(data)
    day_temp_list = [record['temp'] for record in data['hourly']]
    return day_temp_list


def get_forecast_temp_list(latitude, longitude):
    """
    Gets tuple of 2 elements: list of today forecast temperatures and tuple of lists of coming 4 days data
    """
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


def fill_temperatures(session, cls):
    cities = session.query(cls).all()
    for city in cities:
        lat, lon = city.latitude, city.longitude

        historic_temperatures = [get_day_hist_temp(lat, lon, day_num=day) for day in range(-5, 1)]
        forecast_today, forecast_4days = get_forecast_temp_list(lat, lon)

        hist_temps_dumped = (json.dumps(lst) for lst in historic_temperatures[:-1])
        city.historic_5, city.historic_4, city.historic_3, city.historic_2, city.historic_1 = hist_temps_dumped

        today_temperatures = historic_temperatures[-1]
        today_temperatures.extend(forecast_today)
        city.today = json.dumps(today_temperatures)

        forecast_temps_dumped = (json.dumps(lst) for lst in forecast_4days)
        city.forecast_1, city.forecast_2, city.forecast_3, city.forecast_4 = forecast_temps_dumped
    session.commit()

