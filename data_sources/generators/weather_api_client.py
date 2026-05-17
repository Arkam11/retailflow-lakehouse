"""
weather_api_client.py
Fetches real weather data for retail store locations.
Weather affects demand — cold weather → jacket sales up, rain → online orders up.
This enriches the Gold layer with external context for demand forecasting.

Uses OpenWeatherMap free tier (no cost, just needs an API key).
Falls back to simulated data if no API key is configured.

What this teaches:
- External API integration pattern
- Graceful fallback (simulated data when API unavailable)
- Environment variable based configuration (never hardcode API keys)
"""

import json
import os
import random
from datetime import datetime
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
BASE_URL            = "https://api.openweathermap.org/data/2.5/weather"

# Retail store cities — matches the regions in our other datasets
STORE_CITIES = [
    {"city": "London",    "country": "GB", "region": "Europe"},
    {"city": "New York",  "country": "US", "region": "North America"},
    {"city": "Dubai",     "country": "AE", "region": "Middle East"},
    {"city": "Singapore", "country": "SG", "region": "Asia Pacific"},
    {"city": "Mumbai",    "country": "IN", "region": "South Asia"},
]


def fetch_real_weather(city: str, country: str) -> dict | None:
    """Fetch live weather from OpenWeatherMap API."""
    if not OPENWEATHER_API_KEY:
        return None

    try:
        response = requests.get(
            BASE_URL,
            params={"q": f"{city},{country}", "appid": OPENWEATHER_API_KEY, "units": "metric"},
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()

        return {
            "temp_celsius":    data["main"]["temp"],
            "feels_like":      data["main"]["feels_like"],
            "humidity_pct":    data["main"]["humidity"],
            "condition":       data["weather"][0]["main"],
            "condition_desc":  data["weather"][0]["description"],
            "wind_speed_mps":  data["wind"]["speed"],
            "visibility_km":   data.get("visibility", 10000) / 1000,
            "data_source":     "openweathermap_api",
        }
    except Exception as e:
        print(f"  API call failed for {city}: {e} — using simulated data")
        return None


def simulate_weather(region: str) -> dict:
    """
    Simulate realistic weather when API key not available.
    Each region gets climatically appropriate weather ranges.
    """
    climate_profiles = {
        "Europe":        {"temp": (-5, 25),  "humidity": (50, 90), "conditions": ["Clear", "Clouds", "Rain", "Snow"]},
        "North America": {"temp": (-10, 35), "humidity": (30, 80), "conditions": ["Clear", "Clouds", "Rain", "Snow", "Thunderstorm"]},
        "Middle East":   {"temp": (20, 48),  "humidity": (10, 60), "conditions": ["Clear", "Clouds", "Dust"]},
        "Asia Pacific":  {"temp": (15, 38),  "humidity": (60, 95), "conditions": ["Clear", "Clouds", "Rain", "Thunderstorm"]},
        "South Asia":    {"temp": (18, 42),  "humidity": (50, 95), "conditions": ["Clear", "Clouds", "Rain", "Thunderstorm"]},
    }
    profile   = climate_profiles.get(region, climate_profiles["Europe"])
    temp      = round(random.uniform(*profile["temp"]), 1)
    condition = random.choice(profile["conditions"])

    return {
        "temp_celsius":   temp,
        "feels_like":     round(temp - random.uniform(0, 5), 1),
        "humidity_pct":   random.randint(*profile["humidity"]),
        "condition":      condition,
        "condition_desc": condition.lower(),
        "wind_speed_mps": round(random.uniform(0, 15), 1),
        "visibility_km":  round(random.uniform(2, 20), 1),
        "data_source":    "simulated",
    }


def collect_weather_for_all_cities(output_dir: str = "data_sources/sample_data") -> str:
    """
    Collect weather for all store cities and save as JSON Lines.
    This runs daily and feeds the Gold layer demand forecasting model.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    records = []

    for location in STORE_CITIES:
        print(f"  Fetching weather: {location['city']}...")
        weather = fetch_real_weather(location["city"], location["country"]) \
                  or simulate_weather(location["region"])

        records.append({
            "weather_date":  str(datetime.utcnow().date()),
            "city":          location["city"],
            "country":       location["country"],
            "region":        location["region"],
            **weather,
            "extracted_at":  datetime.utcnow().isoformat(),
            "source_system": "weather_service",
        })

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename  = f"{output_dir}/weather_{timestamp}.json"

    with open(filename, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    api_count = sum(1 for r in records if r["data_source"] == "openweathermap_api")
    sim_count = len(records) - api_count
    print(f"✓ Weather collected: {api_count} live, {sim_count} simulated → {filename}")
    return filename


if __name__ == "__main__":
    collect_weather_for_all_cities()
