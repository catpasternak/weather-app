from toolbox.os_tools import path_to_
from toolbox import data_tools


def test_country_name_validation_func():
    assert data_tools.is_country('IT')
    assert data_tools.is_country('US')
    assert data_tools.is_country('RU')
    assert not data_tools.is_country('OO')


def test_coordinate_validation_func():
    assert data_tools.is_coordinate(-45)
    assert data_tools.is_coordinate(0)
    assert data_tools.is_coordinate('156.454635')
    assert not data_tools.is_coordinate(220)
    assert not data_tools.is_coordinate('abc')


def test_find_city_center_func():
    result = data_tools.find_city_center([(1, 1), (1, -1), (-1, 1), (-1, -1)])
    assert result == (0, 0)


def test_get_max_temp_day():
    temp_list = [['day1', 1, 5], ['day2', 2, 12], ['day3', 3, 7]]
    result = data_tools.get_max_temp_day(temp_list)
    assert result == (12, 'day2')


def test_get_max_temp_delta():
    temp_list = [['day1', 1, 5], ['day2', 2, 12], ['day3', 3, 7]]
    result = data_tools.get_max_temp_delta(temp_list)
    assert result == 7


def test_get_min_temp_day():
    temp_list = [['day1', 1, 5], ['day2', 2, 12], ['day3', 3, 7]]
    result = data_tools.get_min_temp_day(temp_list)
    assert result == (1, 'day1')


def test_get_max_minmax_temp_delta():
    temp_list = [['day1', 1, 5], ['day2', 2, 12], ['day3', 3, 7]]
    result = data_tools.get_max_minmax_temp_delta(temp_list)
    assert result == (10, 'day2')
