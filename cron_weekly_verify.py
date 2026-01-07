"""Cron job: Weekly Summary Verification at Friday 12am MYT

Schedule: 0 16 * * 4 (Thursday 16:00 UTC = Friday 00:00 MYT)

Runs weekly to verify daily_sales_summary accuracy:
1. Compares summary totals with raw AcCSD data for last 7 days
2. Identifies and fixes any discrepancies
3. Logs verification results to summary_verification_log
"""
import requests
import os

API_URL = os.getenv("API_URL", "https://flt-kpi-api.onrender.com")
API_KEY = os.getenv("PUSH_API_KEY", "flt-push-2024")

print("=" * 60)
print("[Weekly Verification] Starting summary accuracy check")
print("=" * 60)

# Call the verification endpoint
print("\nCalling verification API...")
response = requests.post(
    f"{API_URL}/api/v1/admin/verify-summary",
    params={"api_key": API_KEY, "days": 7},
    timeout=600  # 10 minutes timeout for verification
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text[:1000]}")

print("\n" + "=" * 60)
print("[Weekly Verification] Complete")
print("=" * 60)
