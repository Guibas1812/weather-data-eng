import requests
from dotenv import load_dotenv
import boto3
import os
from pathlib import Path
import pandas as pd
import json
import time

load_dotenv()  # reads .env, sets env variables

trip_map_url = 'http://api.opentripmap.com/0.1/en/places/radius'
api_key = os.environ.get("OPENTRIPMAP_API_KEY")
radius = 10000 #10KM range

#response = requests.get(trip_map_url,params=params)
#response=response.json()
#response

s3 = boto3.client("s3")
bucket_name = "weather-data-eng" #bucket_name

# get cities from csv
cities_df = pd.read_csv(s3.get_object(
            Bucket=bucket_name,
            Key="silver/reference/cities.csv")['Body'])


for i in range(5): #len(cities_df) #iterate through the cities in the dataframe

    city_name = cities_df['city'][i]
    lat = cities_df['lat'][i]
    lon= cities_df['lng'][i]

    params = {
    'apikey' : api_key ,
    'lang' : 'en' ,
    'lat' : lat ,
    'lon' : lon ,
    'radius' : radius,
    'limit' : 50,
    'rate' : 2
    }

    try:
        response = requests.get(trip_map_url,params=params)
        response.raise_for_status()  # raises an exception if status isn't 200
        activities_data = response.json()
        s3.put_object(
                    Bucket=bucket_name,
                    Key=f"bronze/activities/{city_name}/{city_name}_activities.json",
                    Body=json.dumps(activities_data)
                )

    except Exception as e:
        print(f"Failed to fetch activities for {city_name}: {e}")
        continue

    time.sleep(0.5)


print ("Activities data for all cities has been successfully loaded to the S3 bucket.")

