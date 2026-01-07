"""Cron job: Commission Threshold Check every 30 min (Mon-Sat, 9am-9pm MYT)

Schedule: */30 1-13 * * 1-6 (every 30 min during business hours)

Tracks cumulative daily commission and notifies at thresholds:
- RM50: "Half century! You've earned RM50+ today!"
- RM100: "Triple digits! RM100 commission day!"
- RM200: "On fire! RM200+ commission!"
- RM500: "LEGENDARY! RM500+ commission day!"

Each threshold notifies ONCE per day to avoid spam.
"""
import requests
import os

API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

print(f"[Commission Check] Calling {API_URL}/api/v1/push/commission-threshold-check")

response = requests.post(
    f"{API_URL}/api/v1/push/commission-threshold-check",
    params={"api_key": API_KEY},
    timeout=120
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
