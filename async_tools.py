import json
import asyncio
import aiohttp


API_KEY = 's23N9lets5Gey28fkbpt3ub8v4N6efyk'
BASE_URL = 'http://open.mapquestapi.com/geocoding/v1/reverse'
# BASE_URL = 'http://open.mapquestapi.com/geocoding/v1/reverse?key=KEY&location=30.333472,-81.470448'


async def get_address(session, base_url, api_key, lat, lon, counter=[0]):
    if counter[0] > 3:
        return False
    params = {'key': api_key, 'location': f'{lat},{lon}'}
    async with session.get(base_url, params) as resp:
        if resp.status != 200:
            counter[0] += 1
            await get_address(session, base_url, api_key, lat, lon, counter=counter)
        data = await resp.json()
    if data['info']['statuscode']:
        counter[0] += 1
        await get_address(session, base_url, api_key, lat, lon, counter=counter)
    return await data['results'][0]['locations'][0]['street']


async def get_all_addresses(session, base_url, api_key, coord_list):
    tasks = []

