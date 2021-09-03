import csv
from collections import defaultdict
import json
import os

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from models import Base

from .os_tools import *
from .data_tools import *
from .geo_tools import *
from .weather_tools import *


def start_db_session(db_path):
    engine = create_engine(db_path)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def add_records_to_table(file_path, session, cls):
    for record in read_validated(file_path):
        hotel = cls(name=record[0], country=record[1], city=record[2], latitude=record[3], longitude=record[4])
        session.add(hotel)


def fill_table_from_csv(file_dir, session, cls):
    if session.query(cls).first():
        return True
    for file in os.listdir(file_dir):
        if file.endswith('.csv'):
            add_records_to_table(path_to_(file_dir, file), session, cls)
    session.commit()


def find_major_cities(session, cls):
    stmt = text(
        "select h.COU1, h.C1 from ("
        "select country as COU1, city as C1, count(name) as I from hotels group by COU1, C1"
        ") as h inner join ("
        "select h3.COU3 as COU4, max(h3.K) as m from ("
        "select country as COU3, count(name) as K from hotels group by COU3, city"
        ") as h3 group by h3.COU3) as h4 on (h4.COU4 = h.COU1 and h4.m = h.I)"
                ).columns(cls.country, cls.city)
    query = session.query(cls).from_statement(stmt).all()
    return {obj.country: obj.city for obj in query}


def attach_address_if_in_major_city(hotel, major_cities):
    if hotel.city == major_cities.get(hotel.country) and not hotel.address:
        lat, lon = hotel.latitude, hotel.longitude
        address = get_address(lat, lon)
        hotel.address = address
    return hotel


def fill_addresses_for_major_cities(session, cls, major_cities, threads=4):
    attach_address = partial(attach_address_if_in_major_city, major_cities=major_cities)
    hotels = session.query(cls).all()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        pool.map(attach_address, hotels)
    session.commit()


def get_hotels_coordinates(session, cls, country, city):
    coordinates_list = []
    query = session.query(cls).filter(cls.country == country, cls.city == city).all()
    for hotel in query:
        coordinates_list.append((float(hotel.latitude), float(hotel.longitude)))
    return coordinates_list


def get_major_cities_coordinates(session, cls, major_cities):
    coordinates = defaultdict()
    for country, city in major_cities.items():
        coordinates_list = get_hotels_coordinates(session, cls, country, city)
        coordinates[(country, city)] = find_city_center(coordinates_list)
    return coordinates


def fill_major_cities_table_with_coordinates(session, source_cls, target_cls, major_cities):
    city_centers = get_major_cities_coordinates(session, source_cls, major_cities)
    if not session.query(target_cls).first():
        for city, coords in city_centers.items():
            session.add(target_cls(country=city[0], city=city[1], latitude=coords[0], longitude=coords[1]))
        session.commit()


def fill_major_cities_table_with_temperatures(session, cls, threads=4):
    cities = session.query(cls)
    if cities.first().today:
        return True
    coordinates = [(city.latitude, city.longitude) for city in cities]
    with ThreadPoolExecutor(max_workers=threads) as pool:
        hist_all_days_by_city = pool.map(get_all_hist_temp, coordinates)
        forecast_5days_by_city = pool.map(get_forecast_temp_list, coordinates)

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


def create_and_save_all_plots(session, cls, output_path):
    cities = session.query(cls)
    column_names = [column.key for column in cls.__table__.columns]
    temp_columns = column_names[-10:]
    for city in cities:
        day_temperatures = [json.loads(city.__getattribute__(day)) for day in temp_columns]
        create_and_save_city_temp_plot(city.country, city.city, day_temperatures, output_path)


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
    file_path = path_to_(output_path, 'temperature_analytics.json')
    if not os.path.exists(path_to_(output_path)):
        os.mkdir(path_to_(output_path))
    data_to_write = analyse_statistics(session, cls)
    with open(file_path, 'w') as output_file:
        json.dump(data_to_write, output_file, indent=4)


def yield_filtered_from_db(session, cls, country, city):
    hotels = session.query(cls).filter_by(country=country, city=city)
    for hotel in hotels:
        yield hotel.country, hotel.city, [hotel.name, hotel.address, hotel.latitude, hotel.longitude]


def write_to_city_csv(base_path, session, cls, country, city, cities=[], counter=[0], file_num=[0]):  # check
    if city not in cities:
        counter[0] = 0
        file_num[0] += 1
        cities.append(city)
    db_generator = yield_filtered_from_db(session, cls, country, city)
    for record in db_generator:
        city_folder_path = create_city_folder(base_path, country, city)
        if counter[0] == 100:
            file_num[0] += 1
            counter[0] = 0
        if counter[0] == 0:
            with open(f'{city_folder_path}/hotels_{file_num[0]}.csv', 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['hotel', 'address', 'latitude', 'longitude'])
        with open(f'{city_folder_path}/hotels_{file_num[0]}.csv', 'a', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(record)
        counter[0] += 1


def write_from_db_to_files(base_path, session, csl, major_cities):
    for country, city in major_cities.items():
        write_to_city_csv(base_path, session, cls=csl, country=country, city=city)
