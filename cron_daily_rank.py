"""Cron job: Daily rank and milestone check (9pm MYT)

Schedule: 0 21 * * * (daily at 9pm)

Runs after business hours to check:
1. Rank changes - notify staff who climbed 3+ positions, entered top 10, or became #1
2. Ahead-of-target milestones - notify staff who hit 25%, 50%, 75%, 100% EARLY
"""
import requests
import os

API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

response = requests.post(f"{API_URL}/api/v1/push/daily-rank-check?api_key={API_KEY}")
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")
