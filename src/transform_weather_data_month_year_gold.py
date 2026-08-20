from dotenv import load_dotenv
import boto3
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

"""
Used PySpark for the transformation layer for demonstration purposes; 
Note that at this project's data volume, pandas would also be sufficient — Spark's value would scale with more cities/longer history

"""

load_dotenv()  # reads .env, sets env variables

s3 = boto3.client("s3")
bucket_name = "weather-data-eng"


spark = SparkSession.builder.appName("WeatherDataTransformation").getOrCreate()

response = s3.get_object(Bucket=bucket_name, Key="silver/weather/daily_data.csv")
silver_pd = pd.read_csv(response["Body"])

# convert to Spark DataFrame 
silver_weather_df = spark.createDataFrame(silver_pd)

#silver_weather_df.show(5)

# aggregate daily data to month_year level
month_year_weather_df = silver_weather_df.groupBy("city","month_year").agg({
    "daily_sunshine_seconds": "mean",
    "daily_avg_temperature": "mean",
    "daily_max_temperature": "max",
    "daily_min_temperature": "min",
    "daily_rain": "mean",
    "daily_avg_uv": "mean"
    })

#month_year_weather_df.show(5)

month_year_weather_df = (
    month_year_weather_df
    .withColumnRenamed("avg(daily_sunshine_seconds)", "avg_daily_sunshine_seconds")
    .withColumnRenamed("avg(daily_avg_temperature)", "avg_temperature_celsius")
    .withColumnRenamed("max(daily_max_temperature)", "max_temperature_celsius")
    .withColumnRenamed("min(daily_min_temperature)", "min_temperature_celsius")
    .withColumnRenamed("avg(daily_rain)", "avg_daily_rainfall_mm")
    .withColumnRenamed("avg(daily_avg_uv)", "avg_uv_index")
)

#month_year_weather_df.show(5)

# aggregate daily data to month_year level
month_weather_df = silver_weather_df.groupBy("city","month").agg({
    "daily_sunshine_seconds": "mean",
    "daily_avg_temperature": "mean",
    "daily_max_temperature": "max",
    "daily_min_temperature": "min",
    "daily_rain": "mean",
    "daily_avg_uv": "mean"
    })


month_weather_df = (
    month_weather_df
    .withColumnRenamed("avg(daily_sunshine_seconds)", "avg_daily_sunshine_seconds")
    .withColumnRenamed("avg(daily_avg_temperature)", "avg_temperature_celsius")
    .withColumnRenamed("max(daily_max_temperature)", "max_temperature_celsius")
    .withColumnRenamed("min(daily_min_temperature)", "min_temperature_celsius")
    .withColumnRenamed("avg(daily_rain)", "avg_daily_rainfall_mm")
    .withColumnRenamed("avg(daily_avg_uv)", "avg_uv_index")
)

#month_weather_df.show(5)

month_year_pd = month_year_weather_df.toPandas() # loaded to pandas to avoid S3A connector
month_pd = month_weather_df.toPandas() # loaded to pandas to avoid S3A connector

s3.put_object(
    Bucket=bucket_name,
    Key="gold/fact_weather_monthly_by_year.csv",
    Body=month_year_pd.to_csv(index=False)
)

print(f"Wrote {len(month_year_pd)} rows to gold/fact_weather_monthly_by_year.csv")

s3.put_object(
    Bucket=bucket_name,
    Key="gold/fact_weather_l3y_avg.csv",
    Body=month_pd.to_csv(index=False)
)

print(f"Wrote {len(month_pd)} rows to gold/fact_weather_l3y_avg.csv")

spark.stop()