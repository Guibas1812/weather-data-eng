import requests
from dotenv import load_dotenv
import boto3
from pathlib import Path
import pandas as pd
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

def _fetch_and_land_weather(city_name, lat, lon, start_date, end_date, s3, bucket_name, file_type):
    """To call the API and land the raw response."""

    #get historical weather for that location
    weather_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    weather_params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "rain", "uv_index", "sunshine_duration"]
    }

    weather_response = requests.get(weather_url, params=weather_params)
    weather_response.raise_for_status()
    weather_data = weather_response.json()
    city_name = city_name.replace(" ", "_").lower()

    s3.put_object(
        Bucket=bucket_name,
        Key=f"bronze/weather/{city_name}/{city_name}_{file_type}.json",
        Body=json.dumps(weather_data)
    )

def backfill_weather_to_bronze(start_date="2023-06-01", end_date=None):
    """One-time run"""

    load_dotenv()  # reads .env, sets env variables
    s3 = boto3.client("s3")
    bucket_name = "weather-data-eng" #bucket_name

    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    # get cities from csv
    cities_df = pd.read_csv(s3.get_object(
                Bucket=bucket_name,
                Key="silver/reference/cities.csv")['Body'])

    for city in cities_df['city']: #iterate through the cities in the dataframe
        try:
            geo_url = "https://geocoding-api.open-meteo.com/v1/search"
            geo_params = {"name": city, "count": 1} #get first option from the geocoding API for the city name
            geo_response = requests.get(geo_url, params=geo_params)
            geo_response.raise_for_status()
            geo_data = geo_response.json()

            if "results" not in geo_data: #check results
                print(f"Could not find location: {city}")
            else:
                location = geo_data["results"][0]
                lat = location["latitude"]
                lon = location["longitude"]
                file_type=f"backfill_{start_date}_to_{end_date}"
                _fetch_and_land_weather(city,lat,lon,start_date,end_date,s3,bucket_name,file_type)
                print(f"Found {location['name']}, {location.get('country')} — lat: {lat}, lon: {lon}")
                
        except Exception as e:
            print(f"Failed to backfill {city}: {e}")
            continue

    print("Backfill complete.")
    return "success"


def load_weather_data_to_bronze(execution_date=None):

    load_dotenv()  # reads .env, sets env variables
    s3 = boto3.client("s3")
    bucket_name = "weather-data-eng" #bucket_name

    if execution_date is None:
        execution_date = datetime.today()
    else:
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

    # To get the previous month to fetch API -> go to first day of the current month, subtract one (so land in last day of previous month) and then get the first day of the subtracted one
    first_of_this_month = execution_date.replace(day=1)
    last_month_end = first_of_this_month - relativedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    start_date = last_month_start.strftime("%Y-%m-%d")
    end_date = last_month_end.strftime("%Y-%m-%d")

    # get cities from csv
    cities_df = pd.read_csv(s3.get_object(
                Bucket=bucket_name,
                Key="silver/reference/cities.csv")['Body'])

    for city in cities_df['city']: #iterate through the cities in the dataframe

        try:
            geo_url = "https://geocoding-api.open-meteo.com/v1/search"
            geo_params = {"name": city, "count": 1} #get first option from the geocoding API for the city name
            geo_response = requests.get(geo_url, params=geo_params)
            geo_response.raise_for_status()
            geo_data = geo_response.json()

            if "results" not in geo_data: #check results
                print(f"Could not find location: {city}")
            else:
                location = geo_data["results"][0]
                lat = location["latitude"]
                lon = location["longitude"]
                file_type=last_month_start.strftime('%Y-%m')
                _fetch_and_land_weather(city,lat,lon,start_date,end_date,s3,bucket_name,file_type)
                print(f"Found {location['name']}, {location.get('country')} — lat: {lat}, lon: {lon}")

        except Exception as e:
            print(f"Failed to load {city}: {e}")
            continue

    print(f"Weather data for {start_date} to {end_date} loaded to bronze.")
    return "success"

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        backfill_weather_to_bronze(start_date="2023-06-01", end_date="2026-06-30")
    else:
        load_weather_data_to_bronze()
