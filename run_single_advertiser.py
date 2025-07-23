#!/usr/bin/env python3

from main import get_ad_insights_by_day
import json

# Run the function for advertiser ID 20651
endpoint = "https://api.stackadapt.com/graphql"
advertiser_id = "20651"

print(f"Fetching ad insights for advertiser ID {advertiser_id}...")
result = get_ad_insights_by_day(endpoint, advertiser_id)

if result:
    print("✓ Successfully fetched data")
    print("Response:")
    print(json.dumps(result, indent=2))
else:
    print("✗ Failed to fetch data")