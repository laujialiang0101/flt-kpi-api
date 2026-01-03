"""Cron job: Morning briefing push notification (8am MYT)"""
import requests
import os

# Use internal URL for Render-to-Render communication (faster, no egress)
API_URL = os.getenv("API_URL", "http://flt-kpi-api:10000")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

response = requests.post(f"{API_URL}/api/v1/push/morning-briefing?api_key={API_KEY}")
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
