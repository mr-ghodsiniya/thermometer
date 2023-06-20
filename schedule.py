from celery import schedules

beat_schedule = {
    'call_weather_of_ten_city_every_one_minute': {
        "task": "weather.ten_city",
        "schedule": 60,
        'args': ('tehran', 'joybar', 'gorgan', 'mashhad', 'tabriz',
                 'london', 'liverpool', 'madrid', 'munich', 'shiraz',
                 'ardebil')
    }
}