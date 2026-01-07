"""Cron job: Weekly Summary Verification

Schedule: Friday 8am MYT (0 0 * * 5 UTC)
East Coast Malaysia weekend starts Friday.

Verifies daily_sales_summary accuracy against raw tables.
Fixes any discrepancies found.

Note: MV refresh no longer needed - MVs converted to regular views
that always show fresh data from summary tables.
"""
import requests
import os

API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

print("=" * 60)
print("[Weekly Verification] Starting summary accuracy check")
print("=" * 60)

response = requests.post(
    f"{API_URL}/api/v1/admin/verify-summary",
    params={"api_key": API_KEY, "days": 7},
    timeout=600
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")

print("\n" + "=" * 60)
print("[Weekly Verification] Complete")
print("=" * 60)
