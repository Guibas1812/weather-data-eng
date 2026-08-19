import requests
from dotenv import load_dotenv
import boto3
from pathlib import Path
import pandas as pd
import json

load_dotenv()  # reads .env, sets env variables

s3 = boto3.client("s3")
bucket_name = "weather-data-eng" #bucket_name

response = s3.list_objects_v2(Bucket=bucket_name, Prefix="bronze/activities/")
activity_keys = []
all_rows = []

for obj in response.get("Contents", []):
    if obj["Key"].endswith(".json"):  # check if the file is a JSON file
        # print(obj["Key"])  # prints the key of each JSON file in the list
        activity_keys.append(obj["Key"])
        #print(activities_keys)
    else:
        print(f"Skipping non-JSON file: {obj['Key']}")

for key in activity_keys:
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    activities_data = json.loads(obj["Body"].read())
    city_name = key.split("/")[2]

    for feature in activities_data["features"]:
        #print(feature['properties']['name'])

        all_rows.append({
            'id' : feature['properties']['xid'] ,
            'city' : city_name ,
            'name' : feature['properties']['name'] ,
            'feature' : feature['type'] ,
            'dist' : feature['properties']['dist'] ,
            'rate' : feature['properties']['rate'] ,
            'kinds' : feature['properties']['kinds'] ,
            'lat' : feature['geometry']['coordinates'][1] ,
            'lon' : feature['geometry']['coordinates'][0] ,
        })

silver_activities_df = pd.DataFrame(all_rows)

s3.put_object(
    Bucket=bucket_name,
    Key="silver/activities/activities_data.csv",
    Body=silver_activities_df.to_csv(index=False)
)

print(f"Wrote {len(silver_activities_df)} rows to silver/activities/activities_data.csv")




