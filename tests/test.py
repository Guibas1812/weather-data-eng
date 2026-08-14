import requests
import pandas as pd

# --- Step 1: Get city from user ---
city = input("Enter a city: ")

# --- Step 2: Geocode city name to lat/lon ---
geo_url = "https://geocoding-api.open-meteo.com/v1/search"
geo_params = {"name": city, "count": 1}

geo_response = requests.get(geo_url, params=geo_params)
geo_data = geo_response.json()

if "results" not in geo_data:
    print(f"Could not find location: {city}")
else:
    location = geo_data["results"][0]
    lat = location["latitude"]
    lon = location["longitude"]
    country = location.get("country")
    country_code = location.get("country_code")

    print(f"City: {location['name']}")
    print(f"Country: {country}")
    print(f"Country code: {country_code}")
    print(f"Latitude: {lat}")
    print(f"Longitude: {lon}")

    # --- Step 3: Get historical weather for that location ---
    weather_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    weather_params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2025-06-01",
        "end_date": "2025-06-30",
        "hourly": ["temperature_2m", "rain", "uv_index", "sunshine_duration"]
    }

    weather_response = requests.get(weather_url, params=weather_params)
    weather_data = weather_response.json()
    hourly = weather_data["hourly"]

    # --- Step 4: Put it in a table ---
    df = pd.DataFrame({
        "date": hourly["time"],
        "temperature_2m": hourly["temperature_2m"],
        "rain": hourly["rain"],
        "uv_index": hourly["uv_index"],
        "sunshine_duration": hourly["sunshine_duration"]
    })

    print(df)