from dotenv import load_dotenv
import boto3
from pathlib import Path
import pandas as pd

load_dotenv()  # reads .env, sets env variables

s3 = boto3.client("s3")
bucket_name = "weather-data-eng" #bucket_name

cities_df = pd.read_csv(s3.get_object(
    Bucket=bucket_name,
    Key="bronze/reference/cities.csv")['Body'])

top50_cities = cities_df.sort_values(by=['population'], ascending=False).head(50)

s3.put_object(
    Bucket=bucket_name,
    Key="silver/reference/cities.csv",
    Body=top50_cities.to_csv(index=False)
)
