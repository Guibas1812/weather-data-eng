from dotenv import load_dotenv
import boto3
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.window import Window

"""
Used PySpark for the transformation layer for demonstration purposes; 
Note that at this project's data volume, pandas would also be sufficient — Spark's value would scale with more cities/longer history

"""

load_dotenv()  # reads .env, sets env variables

s3 = boto3.client("s3")
bucket_name = "weather-data-eng"
spark = SparkSession.builder.appName("ActivitiesDataTransformation").getOrCreate()

response = s3.get_object(Bucket=bucket_name, Key="silver/activities/activities_data.csv")
silver_pd = pd.read_csv(response["Body"])

silver_activities_df = spark.createDataFrame(silver_pd)

# silver_activities_df.orderBy(["city", "rate"], ascending=[True, False]).show()

# put rounded lat/lon columns to use for deduplication
silver_activities_df = silver_activities_df.withColumn('lat_rounded', F.round('lat', 3))
silver_activities_df = silver_activities_df.withColumn('lon_rounded', F.round('lon', 3))

# Deduplicate by city and rounded lat/lon, keeping the highest rated attraction for each location
gold_attractions_df = silver_activities_df.withColumn(
    'row_number',
    F.row_number().over(Window.partitionBy('city','lat_rounded','lon_rounded').orderBy(F.desc('rate')))).filter('row_number = 1').drop('lat_rounded', 'lon_rounded', 'row_number').orderBy('city', F.desc('rate'))

gold_attractions_df = gold_attractions_df.withColumn(
    'row_number',
    F.row_number().over(Window.partitionBy('city','name').orderBy(F.desc('rate')))).filter('row_number = 1').drop('row_number').orderBy('city', F.desc('rate'))

#----- attractions gold created -> pass to pandas for later upload to S3
gold_attractions_pd = gold_attractions_df.toPandas()

# explode kinds into individual rows
exploded_df = gold_attractions_df.withColumn(
    "kind", F.explode(F.split(F.col("kinds"), ","))
)
exploded_df = exploded_df.withColumn("kind", F.trim(F.col("kind")))

# Get the top-level categories from the kinds column
exploded_df = exploded_df.withColumn("kind", F.trim(F.col("kind")))

# Top-level categories to keep (belong to kinds hierarchy)
top_level_kinds = [
    "tourist_facilities", "sport", "religion", "other", "natural",
    "industrial_facilities", "historic", "cultural", "architecture",
    "amusements", "adult", "accomodations"
]

# Keep only rows where kind is one of the top-level categories
filtered_df = exploded_df.filter(F.col("kind").isin(top_level_kinds))

gold_activities_by_category_df = filtered_df.select(
    "city", "name", "kind", "rate", "dist", "lat", "lon").orderBy("city","name", "kind", F.desc("rate"))

#----- activity by category gold created -> pass to pandas for later upload to S3
gold_activities_by_category_pd = gold_activities_by_category_df.toPandas()

# Aggregate: distinct attraction count per city per category
gold_activity_counts_df = (
    filtered_df
    .groupBy("city", "kind")
    .agg(F.countDistinct("id").alias("attraction_count"))
    .orderBy("city", F.desc("attraction_count"))
)

#----- activity counts by city and category gold created -> pass to pandas for later upload to S3
gold_activity_counts_pd = gold_activity_counts_df.toPandas()

# s3 uploads
s3.put_object(
    Bucket=bucket_name,
    Key="gold/fact_city_attractions.csv",
    Body=gold_attractions_pd.to_csv(index=False)
)

print(f"Wrote {len(gold_attractions_pd)} rows to gold/fact_city_attractions.csv")

s3.put_object(
    Bucket=bucket_name,
    Key="gold/fact_city_activities_by_category.csv",
    Body=gold_activities_by_category_pd.to_csv(index=False)
)

print(f"Wrote {len(gold_activities_by_category_pd)} rows to gold/fact_city_activities_by_category.csv")

s3.put_object(
    Bucket=bucket_name,
    Key="gold/fact_activity_counts.csv",
    Body=gold_activity_counts_pd.to_csv(index=False)
)

print(f"Wrote {len(gold_activity_counts_pd)} rows to gold/fact_activity_counts.csv")

spark.stop()

