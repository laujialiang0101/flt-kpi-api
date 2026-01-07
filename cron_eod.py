"""Cron job: End of Day Recap at 6pm MYT (Mon-Sat)

Schedule: 0 10 * * 1-6 (10:00 UTC = 18:00 MYT)

Sends end-of-day summary:
- Today's total commission earned
- Daily target achievement status
- Rank movement
- Tomorrow motivation
"""
import requests
import os

API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

print(f"[EOD Recap] Calling daily-progress-check (evening)")

# Evening progress check
response1 = requests.post(
    f"{API_URL}/api/v1/push/daily-progress-check",
    params={"time_of_day": "evening", "api_key": API_KEY},
    timeout=120
)
print(f"Progress Check - Status: {response1.status_code}")
print(f"Response: {response1.text[:300]}")

# Evening recap (leaderboard summary)
print(f"\n[EOD Recap] Calling evening-recap")
response2 = requests.post(
    f"{API_URL}/api/v1/push/evening-recap",
    params={"api_key": API_KEY},
    timeout=120
)
print(f"Evening Recap - Status: {response2.status_code}")
print(f"Response: {response2.text[:300]}")
