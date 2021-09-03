import os

from iso3166 import countries_by_alpha2
from matplotlib import pyplot as plt

from .os_tools import create_city_folder
from .time_tools import *


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


def find_city_center(coordinates_list):
    lat_sum = lon_sum = 0
    count = 0
    for latitude, longitude in coordinates_list:
        lat_sum += latitude
        lon_sum += longitude
        count += 1
    avg_lat, avg_lon = lat_sum/count, lon_sum/count
    return avg_lat, avg_lon


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

    path_to_save = create_city_folder(output_path, country, city)
    file_path = os.path.join(path_to_save, f'{city}_plot.png')

    plt.savefig(file_path)
    plt.clf()


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
