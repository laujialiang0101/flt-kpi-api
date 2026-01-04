"""Cron job: Evening recap push notification (10:30pm MYT)"""
import requests
import os

# Use external URL (internal URL doesn't resolve for Render cron jobs)
API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

response = requests.post(f"{API_URL}/api/v1/push/evening-recap?api_key={API_KEY}")
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
