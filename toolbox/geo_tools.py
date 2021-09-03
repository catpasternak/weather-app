from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import OpenMapQuest

GEOCODE_API_KEY = 's23N9lets5Gey28fkbpt3ub8v4N6efyk'


def get_address(latitude, longitude, api_key=GEOCODE_API_KEY):
    """
    Reverse geocoding function that gets address for given coordinates, uses OpenMapQuest service
    :param latitude: location latitude
    :param longitude: location longitude
    :param api_key: OpenMapQuest API key
    :return: physical address
    :rtype: str
    """
    geolocator = OpenMapQuest(api_key=api_key)
    geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1/20, max_retries=3)
    location = geocode(f'{latitude}, {longitude}', timeout=1)
    return location.address
