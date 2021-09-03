from geopy.geocoders import OpenMapQuest
from geopy.extra.rate_limiter import RateLimiter

GEOCODE_API_KEY = 's23N9lets5Gey28fkbpt3ub8v4N6efyk'


def get_address(latitude, longitude, api_key=GEOCODE_API_KEY):
    geolocator = OpenMapQuest(api_key=api_key)
    geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1/20, max_retries=3)
    location = geocode(f'{latitude}, {longitude}', timeout=1)
    return location.address

