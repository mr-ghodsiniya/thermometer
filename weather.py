import os

from requests import get
from redis import Redis
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

app = Celery('weather', backend='redis://localhost:6382', broker='redis://localhost:6382/0')
app.config_from_object('schedule')

def requesting(city):
    red = Redis(host="localhost", port=6382, db=0, decode_responses=True, charset='UTF-8')
    city_temp = red.get(f'temp_{city}')
    if city_temp is not None:
        return float(city_temp)
    try:
        response = get('https://api.openweathermap.org/data/2.5/weather',
                       {"q": {city}, "appid": os.environ.get("API_KEY")})
        r = int(response.json()['main']['temp']) - 273
        red.set(f'temp_{city}', r, ex=60)
        return r
    except ConnectionError:
        return 'you may have problem with connecting please check and try again!!!'

@app.task
def ten_city(*args):
    cities = ['tehran', 'joybar', 'gorgan', 'mashhad', 'tabriz',
              'london', 'liverpool', 'madrid', 'munich', 'shiraz',
              'ardebil']

    mapping = list(zip(cities, list(map(requesting, cities))))
    return mapping
