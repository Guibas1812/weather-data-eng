import requests
from dotenv import load_dotenv
import boto3
from pathlib import Path
import pandas as pd
import json

city = 'Lisbon'
geo_url = "https://geocoding-api.open-meteo.com/v1/search"
geo_params = {"name": city, "count": 1} #get first option from the geocoding API for the city name
geo_response = requests.get(geo_url, params=geo_params)
geo_data = geo_response.json()
location = geo_data["results"][0]
lat = location["latitude"]
lon = location["longitude"]

#get historical weather for that location
weather_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
weather_params = {
    "latitude": lat,
    "longitude": lon,
    "start_date": "2026-07-01",
    "end_date": "2026-07-01",
    "hourly": ["temperature_2m", "rain", "uv_index", "sunshine_duration"]
}

weather_response = requests.get(weather_url, params=weather_params)
weather_data = weather_response.json()
units_dict = weather_data['hourly_units']

units_df = pd.DataFrame(list(units_dict.items()), columns=["field", "unit"])

load_dotenv()  # reads .env, sets env variables

s3 = boto3.client("s3")
bucket_name = "weather-data-eng" #bucket_name

s3.put_object(
    Bucket=bucket_name,
    Key="bronze/reference/weather_units.csv",
    Body=units_df.to_csv(index=False)
)