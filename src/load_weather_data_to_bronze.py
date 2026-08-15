import requests
from dotenv import load_dotenv
import boto3
from pathlib import Path
import pandas as pd
import json

load_dotenv()  # reads .env, sets env variables

s3 = boto3.client("s3")
bucket_name = "weather-data-eng" #bucket_name

# get cities from csv
cities_df = pd.read_csv(s3.get_object(
            Bucket=bucket_name,
            Key="bronze/reference/cities.csv")['Body'])

for city in cities_df['city'].head(5): #iterate through the cities in the dataframe

    geo_url = "https://geocoding-api.open-meteo.com/v1/search"
    geo_params = {"name": city, "count": 1} #get first option from the geocoding API for the city name
    geo_response = requests.get(geo_url, params=geo_params)
    geo_data = geo_response.json()

    if "results" not in geo_data: #check results
        print(f"Could not find location: {city}")
    else:
        location = geo_data["results"][0]
        country = location["country"]
        country_code = location["country_code"]
        lat = location["latitude"]
        lon = location["longitude"]
        print(f"Found {location['name']}, {location.get('country')} — lat: {lat}, lon: {lon}")

        #get historical weather for that location
        weather_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        weather_params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": "2023-07-01",
            "end_date": "2026-07-31",
            "hourly": ["temperature_2m", "rain", "uv_index", "sunshine_duration"]
        }

        weather_response = requests.get(weather_url, params=weather_params)
        weather_data = weather_response.json()
        city_name = city.replace(" ", "_").lower()
       
        s3.put_object(
            Bucket=bucket_name,
            Key=f"bronze/weather/{city_name}/{city_name}_backfill_2023-07_to_2026-07.json",
            Body=json.dumps(weather_data)
        )

print ("Weather data for all cities has been successfully loaded to the S3 bucket.")
