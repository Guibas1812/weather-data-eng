from dotenv import load_dotenv
import boto3
from pathlib import Path
import pandas as pd

load_dotenv()  # reads .env, sets env variables

s3 = boto3.client("s3")
bucket_name = "weather-data-eng" #bucket_name

cities_df = pd.read_csv(s3.get_object(
    Bucket=bucket_name,
    Key="silver/reference/cities.csv")['Body'])

dim_city_df = cities_df[["city", "lat", "lng", "country", "iso2", "population"]].copy()
dim_city_df.columns = ["city", "lat", "lon", "country", "country_code", "population"] 

s3.put_object(
    Bucket=bucket_name,
    Key="gold/dim_city.csv",
    Body=dim_city_df.to_csv(index=False)
)

print(f"Wrote {len(dim_city_df)} rows to gold/dim_city.csv")