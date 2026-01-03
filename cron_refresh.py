"""Cron job: Refresh materialized views"""
import requests
import os

# Use internal URL for Render-to-Render communication (faster, no egress)
API_URL = os.getenv("API_URL", "http://flt-kpi-api:10000")
API_KEY = os.getenv("REFRESH_API_KEY", "flt-refresh-2024")

response = requests.post(f"{API_URL}/api/v1/admin/refresh-views?api_key={API_KEY}")
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
