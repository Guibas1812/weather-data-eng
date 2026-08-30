import requests
from dotenv import load_dotenv
import boto3
from pathlib import Path
import pandas as pd
import json

def transform_weather_data_to_silver():

    load_dotenv()  # reads .env, sets env variables

    s3 = boto3.client("s3")
    bucket_name = "weather-data-eng" #bucket_name

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="bronze/weather/")
    weather_keys = []
    all_daily_dfs = []

    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".json"):  # check if the file is a JSON file
            # print(obj["Key"])  # prints the key of each JSON file in the list
            weather_keys.append(obj["Key"])
            #print(weather_keys)
        else:
            print(f"Skipping non-JSON file: {obj['Key']}")

    for key in weather_keys:
        try:    
            weather_data = json.loads(s3.get_object(Bucket=bucket_name, Key=key)['Body'].read())
            hourly_data = weather_data['hourly']
            city_name = key.split("/")[2]
            #print(city_name)
            #print(hourly_data)
            df = pd.DataFrame({
                "city": city_name,
                #"country": hourly_data['country'], -- in cities.csv
                #"country_code": hourly_data['country_code'], -- in cities.csv
                "date": pd.to_datetime(hourly_data["time"]).date,
                "temperature_2m": hourly_data["temperature_2m"],
                "rain": hourly_data["rain"],
                "uv_index": hourly_data["uv_index"],
                "sunshine_duration": hourly_data["sunshine_duration"],
                "month": pd.to_datetime(hourly_data["time"]).month,
                "month_year": pd.to_datetime(hourly_data["time"]).strftime("%m_%Y")
                })

            daily_df = df.groupby(by=["city","date","month","month_year"]).agg(
                    daily_sunshine_seconds=("sunshine_duration", "sum"),
                    daily_avg_temperature=("temperature_2m", "mean"),
                    daily_max_temperature=("temperature_2m", "max"),
                    daily_min_temperature=("temperature_2m", "min"),
                    daily_rain=("rain", "sum"),
                    daily_max_uv=("uv_index", "max"),
                ).reset_index()

            all_daily_dfs.append(daily_df)

        except Exception as e:
            print(f"Failed to process {key}: {e}")
            continue

    #print (all_daily_dfs)
    silver_weather_df = pd.concat(all_daily_dfs, ignore_index=True)
    silver_weather_df["daily_sunshine_hours"] = silver_weather_df["daily_sunshine_seconds"] / 3600
    silver_weather_df = silver_weather_df.drop_duplicates(subset=["city", "date"], keep="last")

    s3.put_object(
        Bucket=bucket_name,
        Key="silver/weather/daily_data.csv",
        Body=silver_weather_df.to_csv(index=False)
    )

    print(f"Wrote {len(silver_weather_df)} rows to silver/weather/daily_data.csv")

    return "success"

if __name__ == "__main__":
    transform_weather_data_to_silver()
        
    
