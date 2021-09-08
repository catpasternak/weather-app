import pytest
from toolbox.weather_tools import get_city_timezone, get_all_hist_temp, get_forecast_temp_list

WEATHER_API_KEY = "631f57b7539b1908d2fb62f79486fd95"

URL_CURRENT = "https://api.openweathermap.org/data/2.5/weather"
URL_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"
URL_HISTORIC = "https://api.openweathermap.org/data/2.5/onecall/timemachine"


def test_get_city_timezone_returns_correct_timezone():
    moscow_timezone = get_city_timezone(55.751244, 37.618423, url=URL_CURRENT, api_key=WEATHER_API_KEY)
    assert moscow_timezone == 3*60*60


def test_historic_5day_temperature_returns_reasonable_float_values_for_each_day():
    historic_5days_and_today_results = get_all_hist_temp((55.751244, 37.618423))
    historic_5days_results = list(historic_5days_and_today_results)[1:5]
    for result in historic_5days_results:
        assert all(abs(float(result[i])) < 70 for i in range(len(result)))


def test_forecast_temperature_func_returns_8_reasonable_float_values_for_each_of_4_coming_days():
    forecast_today, forecast_4days = get_forecast_temp_list((55.751244, 37.618423))
    for day_temps in forecast_4days:
        assert len(day_temps) == 8
        assert all(abs(float(day_temps[i])) < 70 for i in range(len(day_temps)))
