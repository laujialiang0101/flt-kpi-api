"""Cron job: Refresh materialized views"""
import requests
import os

# Use external URL (internal URL doesn't resolve for Render cron jobs)
API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("REFRESH_API_KEY", "flt-refresh-2024")

response = requests.post(f"{API_URL}/api/v1/admin/refresh-views?api_key={API_KEY}")
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
