import requests
import pandas as pd

city = "Lisbon"

geo_url = "https://geocoding-api.open-meteo.com/v1/search"
geo_params = {"name": city, "count": 1}

geo_response = requests.get(geo_url, params=geo_params)
#print(geo_response.status_code)
#print(geo_response.json()['results'])

# --- Step 2: Geocode city name to lat/lon ---
geo_data = geo_response.json()
#print(geo_data["results"][0]["name"])

if "results" not in geo_data:
    print(f"Could not find location: {city}")
else:
    location = geo_data["results"][0]
    country = location["country"]
    country_code = location["country_code"]
    lat = location["latitude"]
    lon = location["longitude"]
    print(f"Found {location['name']}, {location.get('country')} — lat: {lat}, lon: {lon}")

    # --- Step 3: Get historical weather for that location ---
    weather_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    weather_params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2022-09-01",
        "end_date": "2022-10-01",
        "hourly": ["temperature_2m", "rain", "uv_index", "sunshine_duration"]
    }

    weather_response = requests.get(weather_url, params=weather_params)
    weather_data = weather_response.json()
  
    hourly_data = weather_data["hourly"]

    # --- Step 4: Put it in a table ---
    df = pd.DataFrame({
        "city": city,
        "country": country,
        "country_code": country_code,
        "date": pd.to_datetime(hourly_data["time"]).date,
        "temperature_2m": hourly_data["temperature_2m"],
        "rain": hourly_data["rain"],
        "uv_index": hourly_data["uv_index"],
        "sunshine_duration": hourly_data["sunshine_duration"],
        "month": pd.to_datetime(hourly_data["time"]).month,
        "month_year": pd.to_datetime(hourly_data["time"]).strftime("%m_%Y")})

    daily_df = df.groupby(by=["city", "country", "country_code", "date","month","month_year"]).agg(
        daily_sunshine_seconds=("sunshine_duration", "sum"),
        daily_avg_temperature=("temperature_2m", "mean"),
        daily_max_temperature=("temperature_2m", "max"),
        daily_min_temperature=("temperature_2m", "min"),
        daily_rain=("rain", "sum"),
        daily_avg_uv=("uv_index", "max"),
    ).reset_index()

    month_year_df = daily_df.groupby(by=["city", "country", "country_code", "month_year","month"]).agg(
        avg_daily_sunshine_hours=("daily_sunshine_seconds", lambda x: (x / 3600).mean()),
        avg_temperature=("daily_avg_temperature", "mean"),
        avg_max_temperature=("daily_max_temperature", "mean"),
        avg_min_temperature=("daily_min_temperature", "mean"),
        avg_daily_rain=("daily_rain", "mean"),
        avg_uv_index=("daily_avg_uv", "mean"),
    ).reset_index()


    print(month_year_df.head())
