from dotenv import load_dotenv
import boto3
from pathlib import Path
import pandas as pd

load_dotenv()  # reads .env, sets env variables

s3 = boto3.client("s3")
bucket_name = "weather-data-eng" #bucket_name

project_root = Path(__file__).resolve().parent.parent #get the root of the project
cities_df = pd.read_csv(project_root / "data" / "simplemaps_worldcities_basicv1.91.2" / "worldcities.csv") # check if correctly assigned

s3.put_object(
    Bucket=bucket_name,
    Key="bronze/reference/cities.csv",
    Body=cities_df.to_csv(index=False)
)

