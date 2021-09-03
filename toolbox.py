import os
import csv
import json
from zipfile import ZipFile
import requests
from iso3166 import countries_by_alpha2
from sqlalchemy.sql import text
from geopy.geocoders import OpenMapQuest
from geopy.extra.rate_limiter import RateLimiter
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from collections import defaultdict
from time_tools import *
from matplotlib import pyplot as plt
import numpy as np

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import *

GEOCODE_API_KEY = 's23N9lets5Gey28fkbpt3ub8v4N6efyk'
WEATHER_API_KEY = "631f57b7539b1908d2fb62f79486fd95"

URL_CURRENT = "https://api.openweathermap.org/data/2.5/weather"
URL_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"
URL_HISTORIC = "https://api.openweathermap.org/data/2.5/onecall/timemachine"


def start_db_session(db_path):
    engine = create_engine(db_path)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


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
        return True
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
    # print('started main fill addresses function')
    attach_address = partial(attach_address_if_in_major_city, major_cities=major_cities)
    hotels = session.query(cls).all()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        pool.map(attach_address, hotels)
    session.commit()
    # print(time.time() - s, 'seconds took geocoding execution with', threads, 'threads')


################# Calculate city centers coordinates ###########################################


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


def fill_temp_min_max(session, cls):  # REFACTOR WITH MULTITHREADING
    cities = session.query(cls)
    if cities.first().today:
        return True
    coordinates = [(city.latitude, city.longitude) for city in cities]
    with ThreadPoolExecutor(max_workers=4) as pool:
        hist_all_days_by_city = pool.map(get_all_hist_temp, coordinates)  # 7 generators x 6 lists x <=24 values
        forecast_5days_by_city = pool.map(get_forecast_temp_list, coordinates)  # 7 gen x 1 tuple (list, list(lists))

    for city in cities:
        hist_temp_list = list(next(hist_all_days_by_city))
        forecast_today, forecast_4days = list(next(forecast_5days_by_city))

        hist_temp_ranges = (json.dumps((min(lst), max(lst))) for lst in hist_temp_list[:-1])
        forecast_temp_ranges = (json.dumps((min(lst), max(lst))) for lst in forecast_4days)
        today_temperatures = hist_temp_list[-1] + forecast_today
        today_temp_range = json.dumps((min(today_temperatures), max(today_temperatures)))

        city.historic_5, city.historic_4, city.historic_3, city.historic_2, city.historic_1 = hist_temp_ranges
        city.forecast_1, city.forecast_2, city.forecast_3, city.forecast_4 = forecast_temp_ranges
        city.today = today_temp_range

    session.commit()


########### Create plots ##############################################################################################


def create_city_folder(country, city, output_path):
    if not os.path.exists(os.path.join(os.path.dirname(__file__), output_path)):
        os.mkdir(os.path.join(os.path.dirname(__file__), output_path))
    folder_path = os.path.join(os.path.dirname(__file__), output_path, country, city)
    if not os.path.exists(folder_path):
        os.mkdir(os.path.join(os.path.dirname(__file__), output_path, country))
        os.mkdir(os.path.join(os.path.dirname(__file__), output_path, country, city))
    return folder_path


def create_and_save_city_temp_plot(country, city, temperature_lists, output_path):
    x = [i for i in range(1, 11)]
    y1 = [max(day) for day in temperature_lists]
    y2 = [min(day) for day in temperature_lists]
    today = datetime.datetime.utcnow().date()
    days = [str(today + datetime.timedelta(days=-5+i)) for i in range(10)]

    plt.title(f'Temperature in {city} by day')
    plt.xlabel('Day of observation')
    plt.ylabel('Temperature in Celcius')
    plt.xticks(x, days, rotation=20)
    plt.plot(x, y1, color='red', label='max')
    plt.plot(x, y2, color='blue', label='min')
    plt.grid()
    plt.legend()

    path_to_save = create_city_folder(country, city, output_path)
    file_path = os.path.join(path_to_save, f'{city}_plot.png')

    plt.savefig(file_path)
    plt.clf()


def create_and_save_all_plots(session, cls, output_path):
    cities = session.query(cls)
    column_names = [column.key for column in cls.__table__.columns]
    temp_columns = column_names[-10:]
    for city in cities:
        day_temperatures = [json.loads(city.__getattribute__(day)) for day in temp_columns]
        create_and_save_city_temp_plot(city.country, city.city, day_temperatures, output_path)


########### Analytics #################################################################################################


def get_cities_statistics(session, cls):
    cities = session.query(cls)
    column_names = [column.key for column in cls.__table__.columns]
    temp_columns = column_names[-10:]
    today = datetime.datetime.utcnow().date()
    dates = [str(today + datetime.timedelta(days=-5+i)) for i in range(10)]
    cities_statistics = []
    for city in cities:
        min_temperatures = [json.loads(city.__getattribute__(day))[0] for day in temp_columns]
        max_temperatures = [json.loads(city.__getattribute__(day))[1] for day in temp_columns]
        city_10days_temp = list(zip(dates, min_temperatures, max_temperatures))
        city_statistics = get_city_statistics(city_10days_temp)
        cities_statistics.append((city.country, city.city, city_statistics))
    return cities_statistics


def get_max_temp_day(city_10days_temp):
    max_temp = -100
    max_temp_day = None
    for day_data in city_10days_temp:
        if day_data[2] > max_temp:
            max_temp_day = day_data[0]
            max_temp = day_data[2]
    return max_temp, max_temp_day,

def get_max_temp_delta(city_10days_temp):
    max_temp_list = [day_data[2] for day_data in city_10days_temp]
    max_temp_delta = max(max_temp_list) - min(max_temp_list)
    return max_temp_delta

def get_min_temp_day(city_10days_temp):
    min_temp = 100
    min_temp_day = None
    for day_data in city_10days_temp:
        if day_data[1] < min_temp:
            min_temp_day = day_data[0]
            min_temp = day_data[1]
    return min_temp, min_temp_day,

def get_max_minmax_temp_delta(city_10days_temp):
    max_minmax_delta = 0
    max_minmax_delta_day = None
    for day_data in city_10days_temp:
        if day_data[2] - day_data[1] > max_minmax_delta:
            max_minmax_delta_day = day_data[0]
            max_minmax_delta = day_data[2] - day_data[1]
    return max_minmax_delta, max_minmax_delta_day,

def get_city_statistics(city_10days_temp):
    indicator1 = get_max_temp_day(city_10days_temp)
    indicator2 = get_max_temp_delta(city_10days_temp)
    indicator3 = get_min_temp_day(city_10days_temp)
    indicator4 = get_max_minmax_temp_delta(city_10days_temp)
    return indicator1, indicator2, indicator3, indicator4


def analyse_statistics(session, cls):
    cities_statistics = get_cities_statistics(session, cls)

    countries = [data[0] for data in cities_statistics]
    cities = [data[1] for data in cities_statistics]
    max_temps = [data[2][0][0] for data in cities_statistics]
    max_temp_days = [data[2][0][1] for data in cities_statistics]
    max_temp_max_deltas = [data[2][1] for data in cities_statistics]
    min_temps = [data[2][2][0] for data in cities_statistics]
    min_temp_days = [data[2][2][1] for data in cities_statistics]
    max_minmax_deltas = [data[2][3][0] for data in cities_statistics]
    max_minmax_delta_days = [data[2][3][1] for data in cities_statistics]

    max_temp_index = np.argmax(max_temps)
    max_temp_max_delta_index = np.argmax(max_temp_max_deltas)
    min_temp_index = np.argmin(min_temps)
    max_minmax_delta_index = np.argmax(max_minmax_deltas)

    json_results = {
        'Maximal temperature': {
            'temperature': max(max_temps),
            'day': max_temp_days[max_temp_index],
            'country': countries[max_temp_index],
            'city': cities[max_temp_index]
        },
        'Maximal variation of maximal temperature': {
            'variation': max(max_temp_max_deltas),
            'country': countries[max_temp_max_delta_index],
            'city': cities[max_temp_max_delta_index]
        },
        'Minimal temperature': {
            'temperature': min(min_temps),
            'day': min_temp_days[min_temp_index],
            'country': countries[min_temp_index],
            'city': cities[min_temp_index]
        },
        'Maximal day temperature variation': {
            'variation': max(max_minmax_deltas),
            'day': max_minmax_delta_days[max_minmax_delta_index],
            'country': countries[max_minmax_delta_index],
            'city': cities[max_minmax_delta_index]
        }
    }

    return json_results


def write_temperature_analytics(session, cls, output_path):
    file_path = combine_path(output_path, 'temperature_analytics')
    data_to_write = analyse_statistics(session, cls)
    with open(file_path, 'w') as output_file:
        json.dump(data_to_write, output_file, indent=4)


############# CSV #####################################################################################################


def yield_records_from_table(session, target_class, majors_cities):  # check
    hotels = session.query(target_class)
    for hotel in hotels:
        if hotel.address and majors_cities.get(hotel.country) == hotel.city:
            yield hotel.country, hotel.city, [hotel.name, hotel.address, hotel.latitude, hotel.longitude]

# gen = yield_records_from_table(session, Hotel)


def yield_filtered_from_db(session, cls, country, city):
    hotels = session.query(cls).filter_by(country=country, city=city)
    for hotel in hotels:
        yield hotel.country, hotel.city, [hotel.name, hotel.address, hotel.latitude, hotel.longitude]


def sync_write_to_csv(session, csl, majors_cities):
    generator_list = []
    for country, city in majors_cities.items():
        db_generator = yield_filtered_from_db(session, cls, country, city)
        generator_list.append(db_generator)



def write_to_city_csv(base_path, session, cls, country, city, cities=[], counter=[0], file_num=[0]):  # check
    if city not in cities:
        counter[0] = 0
        file_num[0] += 1
        cities.append(city)
    db_generator = yield_filtered_from_db(session, cls, country, city)
    for record in db_generator:
        path_to_file = os.path.join(os.path.dirname(__file__), base_path, country, city)
        if not os.path.exists(path_to_file):
            if not os.path.exists(os.path.join(os.path.dirname(__file__), base_path, country)):
                os.mkdir(os.path.join(os.path.dirname(__file__), base_path, country))
            os.mkdir(os.path.join(os.path.dirname(__file__), base_path, country, city))
        if counter[0] == 100:
            file_num[0] += 1
            counter[0] = 0
        if counter[0] == 0:
            with open(f'{path_to_file}/hotels_{file_num[0]}.csv', 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['hotel', 'address', 'latitude', 'longitude'])
        with open(f'{path_to_file}/hotels_{file_num[0]}.csv', 'a', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(record)
        counter[0] += 1



def write_from_db_to_files(base_path, session, csl, major_cities):
    for country, city in major_cities.items():
        write_to_city_csv(base_path, session, cls=csl, country=country, city=city)
