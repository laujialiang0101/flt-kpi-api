"""Cron job: Refresh materialized views

Also runs weekly summary verification on Friday mornings (East Coast Malaysia).
Friday = weekend start, good time to verify last week's data.
"""
import requests
import os
from datetime import datetime, timezone, timedelta

# Use external URL (internal URL doesn't resolve for Render cron jobs)
API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("REFRESH_API_KEY", "flt-refresh-2024")
PUSH_API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

# Get current time in MYT (UTC+8)
myt = timezone(timedelta(hours=8))
now_myt = datetime.now(myt)

print(f"[MV Refresh] Current time: {now_myt.strftime('%Y-%m-%d %H:%M %Z')} (Day: {now_myt.strftime('%A')})")

# 1. Always refresh MVs
print("[MV Refresh] Refreshing materialized views...")
response = requests.post(
    f"{API_URL}/api/v1/admin/refresh-views",
    params={"api_key": API_KEY},
    timeout=1800  # 30 min timeout
)
print(f"MV Refresh Status: {response.status_code}")
print(f"Response: {response.text[:500]}")

# 2. Run weekly verification on Friday morning (8am-9am MYT = first cron runs of Friday)
# Friday is start of weekend in East Coast Malaysia
if now_myt.weekday() == 4 and now_myt.hour < 9:  # Friday, before 9am
    print("\n" + "=" * 50)
    print("[Weekly Verification] Friday morning - running weekly check")
    print("=" * 50)

    verify_response = requests.post(
        f"{API_URL}/api/v1/admin/verify-summary",
        params={"api_key": PUSH_API_KEY, "days": 7},
        timeout=600  # 10 min timeout
    )
    print(f"Verification Status: {verify_response.status_code}")
    print(f"Response: {verify_response.text[:500]}")
else:
    print(f"[Weekly Verification] Not Friday morning, skipping (day={now_myt.weekday()}, hour={now_myt.hour})")
