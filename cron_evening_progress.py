"""Cron job: Evening progress check (6pm MYT, Mon-Sat)

Schedule: 0 18 * * 1-6 (Monday to Saturday at 6pm)

Sends end-of-day summary comparing today's sales vs daily target:
- Staff hit target: Celebration message
- Staff close (80%+): Final push encouragement
- Staff behind: Positive message for tomorrow
"""
import requests
import os

API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

response = requests.post(
    f"{API_URL}/api/v1/push/daily-progress-check",
    params={"time_of_day": "evening", "api_key": API_KEY}
)
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
