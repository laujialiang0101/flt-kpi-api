"""Cron job: Midday progress check (12pm MYT, Mon-Sat)

Schedule: 0 12 * * 1-6 (Monday to Saturday at noon)

Sends midday check-in comparing today's sales vs daily target:
- Staff ahead of pace: Encouraging to maintain momentum
- Staff on track: Positive reinforcement
- Staff behind: Motivating afternoon comeback
"""
import requests
import os

API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

response = requests.post(
    f"{API_URL}/api/v1/push/daily-progress-check",
    params={"time_of_day": "midday", "api_key": API_KEY}
)
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
