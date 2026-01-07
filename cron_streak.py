"""Cron job: Daily streak check (11pm MYT)

Schedule: 0 23 * * * (daily at 11pm)

Checks for streak milestones:
- 3-day streak
- 5-day streak
- 7-day streak (weekly warrior)
- 14-day streak (two week titan)
- 30-day streak (monthly master)
"""
import requests
import os

# Use external URL (internal URL doesn't resolve for Render cron jobs)
API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

response = requests.post(f"{API_URL}/api/v1/push/streak-check?api_key={API_KEY}")
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
