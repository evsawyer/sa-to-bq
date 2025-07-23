import requests
import json
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def ping_graphql_api(endpoint_url: str, query: str, variables: dict = None, headers: dict = None):
    """
    Send a raw GraphQL request to the specified endpoint
    """
    payload = {
        "query": query,
        "variables": variables or {}
    }
    
    # Get API key from environment
    api_key = os.getenv('STACKADAPT_API_KEY')
    if not api_key:
        print("Warning: STACKADAPT_API_KEY not found in environment variables")
    
    default_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}" if api_key else ""
    }
    
    if headers:
        default_headers.update(headers)
    
    try:
        response = requests.post(
            endpoint_url,
            json=payload,
            headers=default_headers,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"GraphQL request failed: {e}")
        return None


def get_all_advertiser_ids(endpoint_url: str):
    """
    Fetch all advertiser IDs from the GraphQL API
    """
    query = """
        query GetAllAdvertiserIds {
          advertisers(first: 100) {
            edges {
              node {
                id
                name
              }
            }
          }
        }
    """
    
    result = ping_graphql_api(endpoint_url, query)
    
    if result and 'data' in result and 'advertisers' in result['data']:
        advertiser_ids = []
        for edge in result['data']['advertisers']['edges']:
            advertiser_ids.append(edge['node']['id'])
        return advertiser_ids
    else:
        print("Failed to fetch advertiser IDs")
        return []


def get_ad_insights_by_day(endpoint_url: str, advertiser_id: str):
    """
    Fetch ad insights for a single advertiser ID
    """
    query = """
        query GetAdInsightsByDay($ids: [ID!]!) {
          campaignGroupInsight(
            attributes: [AD, DATE]
            date: {
              from: "2020-06-01"
              to: "2025-07-31"
            }
            filterBy: {
              advertiserIds: $ids
            }
          ) {
            ... on CampaignGroupInsightOutcome {
              records {
                edges {
                  node {
                    attributes {
                      ad {
                        id
                        name
                      }
                      date
                    }
                    campaignGroup {
                      id
                      name
                    }
                    metrics {
                      impressions
                      clicks
                      cost
                    }
                  }
                }
              }
            }
          }
        }
    """
    
    variables = {"ids": [advertiser_id]}
    result = ping_graphql_api(endpoint_url, query, variables)
    
    return result


def main():
    endpoint = "https://api.stackadapt.com/graphql"
    
    # Step 1: Get all advertiser IDs
    print("Fetching all advertiser IDs...")
    advertiser_ids = get_all_advertiser_ids(endpoint)
    
    if not advertiser_ids:
        print("No advertiser IDs found. Exiting.")
        return
    
    print(f"Found {len(advertiser_ids)} advertiser IDs: {advertiser_ids}")
    
    # Step 2: Get ad insights for each advertiser ID individually
    print("Fetching ad insights for each advertiser...")
    all_results = []
    
    for i, advertiser_id in enumerate(advertiser_ids, 1):
        print(f"Processing advertiser {i}/{len(advertiser_ids)}: {advertiser_id}")
        result = get_ad_insights_by_day(endpoint, advertiser_id)
        
        if result:
            all_results.append(result)
            print(f"✓ Successfully fetched data for advertiser {advertiser_id}")
        else:
            print(f"✗ Failed to fetch data for advertiser {advertiser_id}")
        
        # Add delay between requests to avoid rate limiting
        if i < len(advertiser_ids):  # Don't delay after the last request
            time.sleep(1)
    
    # Step 3: Display aggregated results
    if all_results:
        print(f"\nSuccessfully processed {len(all_results)} advertisers")
        print("Combined Ad Insights Response:")
        print(json.dumps(all_results, indent=2))
    else:
        print("Failed to get ad insights from any advertiser")


if __name__ == "__main__":
    main()
