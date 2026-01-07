"""Cron job: Daily Milestones Check at 9pm MYT

Schedule: 0 13 * * * (13:00 UTC = 21:00 MYT)

Runs after business hours to check:
1. Rank changes - notify staff who climbed 3+ positions, entered top 10, or became #1
2. Target milestones - notify staff who hit 25%, 50%, 75%, 100% ahead of schedule
3. Streak milestones - notify staff who hit 3, 7, 14, 30 day streaks
4. First-time achievements - first sale, first HB, first top 10, etc.
"""
import requests
import os

API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

print("=" * 60)
print("[Daily Milestones] Starting comprehensive milestone check")
print("=" * 60)

# 1. Rank changes and target milestones
print("\n[1/3] Checking rank changes and target milestones...")
response1 = requests.post(
    f"{API_URL}/api/v1/push/daily-rank-check",
    params={"api_key": API_KEY},
    timeout=300
)
print(f"Rank/Milestone Check - Status: {response1.status_code}")
print(f"Response: {response1.text[:400]}")

# 2. Streak milestones
print("\n[2/3] Checking streak milestones...")
response2 = requests.post(
    f"{API_URL}/api/v1/push/streak-check",
    params={"api_key": API_KEY},
    timeout=120
)
print(f"Streak Check - Status: {response2.status_code}")
print(f"Response: {response2.text[:400]}")

# 3. First-time achievements
print("\n[3/3] Checking first-time achievements...")
response3 = requests.post(
    f"{API_URL}/api/v1/push/first-time-achievements",
    params={"api_key": API_KEY},
    timeout=120
)
print(f"First-Time Check - Status: {response3.status_code}")
print(f"Response: {response3.text[:400]}")

print("\n" + "=" * 60)
print("[Daily Milestones] Complete")
print("=" * 60)
