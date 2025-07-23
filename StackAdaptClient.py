import requests
import json
import os
import time
from dotenv import load_dotenv
from typing import Optional, Dict, List, Any

# Load environment variables from .env file
load_dotenv()


class StackAdaptClient:
    """
    Client for interacting with the StackAdapt GraphQL API
    """
    
    def __init__(self, api_key: Optional[str] = None, endpoint: Optional[str] = None):
        """
        Initialize the StackAdapt client
        
        Args:
            api_key: StackAdapt API key (defaults to env var STACKADAPT_API_KEY)
            endpoint: GraphQL endpoint URL (defaults to production endpoint)
        """
        self.api_key = api_key or os.getenv('STACKADAPT_API_KEY')
        if not self.api_key:
            raise ValueError("STACKADAPT_API_KEY not found. Please provide it or set it in environment variables")
        
        self.endpoint = endpoint or "https://api.stackadapt.com/graphql"
        self.session = requests.Session()
        self._setup_headers()
    
    def _setup_headers(self):
        """Setup default headers for all requests"""
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        })
    
    def execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Execute a GraphQL query
        
        Args:
            query: GraphQL query string
            variables: Optional variables for the query
            
        Returns:
            Response data or None if failed
        """
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        try:
            response = self.session.post(
                self.endpoint,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"GraphQL request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            return None
    
    def get_all_advertiser_ids(self) -> List[str]:
        """
        Fetch all advertiser IDs from the GraphQL API
        
        Returns:
            List of advertiser IDs
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
        
        result = self.execute_query(query)
        
        if result and 'data' in result and 'advertisers' in result['data']:
            advertiser_ids = []
            for edge in result['data']['advertisers']['edges']:
                advertiser_ids.append(edge['node']['id'])
            return advertiser_ids
        else:
            print("Failed to fetch advertiser IDs")
            return []
    
    def get_ad_insights_by_day_single(self, advertiser_id: str, 
                                      date_from: str = "2020-06-01", 
                                      date_to: str = "2020-10-30") -> Optional[Dict[str, Any]]:
        """
        Fetch ad insights for a single advertiser ID
        
        Args:
            advertiser_id: The advertiser ID to fetch insights for
            date_from: Start date (YYYY-MM-DD format)
            date_to: End date (YYYY-MM-DD format)
            
        Returns:
            Query result or None if failed
        """
        query = """
            query GetAdInsightsByDay($ids: [ID!]!, $dateFrom: ISO8601Date!, $dateTo: ISO8601Date!) {
              campaignGroupInsight(
                attributes: [AD, DATE]
                date: {
                  from: $dateFrom
                  to: $dateTo
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
                            campaign {
                              id
                              name
                              campaignGoal {
                                goalsConnection(first:1) {
                                  edges {
                                    node {
                                      goalType
                                    }
                                  }
                                }
                              }
                              goalType
                              campaignGroup {
                                id
                                name
                                advertiser {
                                  id
                                  name
                                }
                              }
                            }
                          }
                          date
                        }
                        metrics {
                          clicks
                          clickConversions
                          engagements
                          videoStarts
                          videoQ1Playbacks
                          videoQ2Playbacks
                          videoQ3Playbacks
                          videoCompletions
                          impressions
                          frequency
                          cost
                        }
                      }
                    }
                  }
                }
              }
            }
        """
        
        variables = {
            "ids": [advertiser_id],
            "dateFrom": date_from,
            "dateTo": date_to
        }
        
        return self.execute_query(query, variables)
    
    def get_ad_insights_by_day_bulk(self, advertiser_ids: List[str], 
                                    date_from: str = "2020-06-01", 
                                    date_to: str = "2020-10-30") -> Optional[Dict[str, Any]]:
        """
        Fetch ad insights for multiple advertiser IDs in one query
        
        Args:
            advertiser_ids: List of advertiser IDs to fetch insights for
            date_from: Start date (YYYY-MM-DD format)
            date_to: End date (YYYY-MM-DD format)
            
        Returns:
            Query result or None if failed
        """
        query = """
          query GetAdInsightsByDay($ids: [ID!]!, $dateFrom: ISO8601Date!, $dateTo: ISO8601Date!) {
            campaignGroupInsight(
              attributes: [AD, DATE]
              date: {
                from: $dateFrom
                to: $dateTo
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
                          campaign {
                            id
                            name
                            campaignGoal {
                              goalsConnection(first:1) {
                                edges {
                                  node {
                                    goalType
                                  }
                                }
                              }
                            }
                            goalType
                            campaignGroup {
                              id
                              name
                              advertiser {
                                id
                                name
                              }
                            }
                          }
                        }
                        date
                      }
                      metrics {
                        clicks
                        clickConversions
                        engagements
                        videoStarts
                        videoQ1Playbacks
                        videoQ2Playbacks
                        videoQ3Playbacks
                        videoCompletions
                        impressions
                        frequency
                        cost
                      }
                    }
                  }
                }
              }
            }
          }
        """
        
        variables = {
            "ids": advertiser_ids,
            "dateFrom": date_from,
            "dateTo": date_to
        }
        
        return self.execute_query(query, variables)
    
    def _check_data_retrieved(self, result: Dict[str, Any], advertiser_id: str) -> bool:
        """
        Check if the GraphQL response contains actual data records
        
        Args:
            result: GraphQL response
            advertiser_id: The advertiser ID for logging purposes
            
        Returns:
            True if data records were found, False otherwise
        """
        try:
            # Navigate through the GraphQL response structure
            if (result and 
                'data' in result and 
                'campaignGroupInsight' in result['data'] and 
                'records' in result['data']['campaignGroupInsight'] and
                'edges' in result['data']['campaignGroupInsight']['records']):
                
                edges = result['data']['campaignGroupInsight']['records']['edges']
                record_count = len(edges)
                
                if record_count > 0:
                    print(f"  → Found {record_count} records for advertiser {advertiser_id}")
                    return True
                else:
                    print(f"  → No records found for advertiser {advertiser_id}")
                    return False
            else:
                print(f"  → Invalid response structure for advertiser {advertiser_id}")
                return False
                
        except Exception as e:
            print(f"  → Error checking data for advertiser {advertiser_id}: {e}")
            return False
    
    def fetch_all_ad_insights(self, use_bulk: bool = False, delay_between_requests: float = 1.0, 
                             date_from: str = "2020-06-01", date_to: str = "2020-10-30") -> List[Dict[str, Any]]:
        """
        Fetch ad insights for all advertisers using either single requests or bulk
        
        Args:
            use_bulk: If True, use bulk query for all advertisers. If False, use individual queries
            delay_between_requests: Delay in seconds between requests (only used for single mode)
            date_from: Start date (YYYY-MM-DD format)
            date_to: End date (YYYY-MM-DD format)
            
        Returns:
            List of results for all advertisers
        """
        # Step 1: Get all advertiser IDs
        print("Fetching all advertiser IDs...")
        advertiser_ids = self.get_all_advertiser_ids()
        
        if not advertiser_ids:
            print("No advertiser IDs found. Exiting.")
            return []
        
        print(f"Found {len(advertiser_ids)} advertiser IDs: {advertiser_ids}")
        
        if use_bulk:
            # Step 2a: Get ad insights for all advertisers in one bulk query
            print(f"Fetching ad insights for all {len(advertiser_ids)} advertisers in bulk...")
            result = self.get_ad_insights_by_day_bulk(advertiser_ids, date_from, date_to)
            
            if result:
                # Check if actual data was retrieved
                data_retrieved = self._check_data_retrieved(result, f"bulk query ({len(advertiser_ids)} advertisers)")
                if data_retrieved:
                    print(f"✓ Successfully fetched bulk data for all advertisers")
                    return [result]
                else:
                    print(f"⚠ No data found in bulk query (empty result set)")
                    return []
            else:
                print(f"✗ Failed to fetch bulk data")
                return []
        else:
            # Step 2b: Get ad insights for each advertiser ID individually
            print("Fetching ad insights for each advertiser...")
            all_results = []
            
            for i, advertiser_id in enumerate(advertiser_ids, 1):
                print(f"Processing advertiser {i}/{len(advertiser_ids)}: {advertiser_id}")
                result = self.get_ad_insights_by_day_single(advertiser_id, date_from, date_to)
                
                if result:
                    # Check if actual data was retrieved
                    data_retrieved = self._check_data_retrieved(result, advertiser_id)
                    if data_retrieved:
                        all_results.append(result)
                        print(f"✓ Successfully fetched data for advertiser {advertiser_id}")
                    else:
                        print(f"⚠ No data found for advertiser {advertiser_id} (empty result set)")
                else:
                    print(f"✗ Failed to fetch data for advertiser {advertiser_id}")
                
                # Add delay between requests to avoid rate limiting
                if i < len(advertiser_ids):  # Don't delay after the last request
                    time.sleep(delay_between_requests)
            
            return all_results
    
    def test_connection(self) -> bool:
        """
        Test the connection to the GraphQL API
        
        Returns:
            True if connection is successful, False otherwise
        """
        query = """
            query TestConnection {
              __schema {
                types {
                  name
                }
              }
            }
        """
        
        result = self.execute_query(query)
        return result is not None


def main():
    """Example usage of the StackAdaptClient"""
    try:
        # Initialize the client
        client = StackAdaptClient()
        
        # Test connection
        print("Testing connection to StackAdapt API...")
        if not client.test_connection():
            print("Failed to connect to StackAdapt API. Please check your credentials.")
            return
        
        print("Connection successful!")
        
        # Fetch all ad insights (use use_bulk=True for bulk mode)
        all_results = client.fetch_all_ad_insights(use_bulk=False)
        
        # Display results
        if all_results:
            print(f"\nSuccessfully processed {len(all_results)} advertisers")
            print("Combined Ad Insights Response:")
            print(json.dumps(all_results, indent=2))
        else:
            print("Failed to get ad insights from any advertiser")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()