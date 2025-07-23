#!/usr/bin/env python3
"""
StackAdapt to BigQuery Pipeline
Fetches StackAdapt ad performance data and loads it to BigQuery
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import logging
from dotenv import load_dotenv
import time
import argparse
import pandas_gbq

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our custom clients
from StackAdaptClient import StackAdaptClient
from BigQueryClient import BigQueryClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class StackAdaptToBigQueryPipeline:
    """Pipeline for syncing StackAdapt ads data to BigQuery"""
    
    def __init__(self, dataset_id: str = "raw_ads", project_id: str = None):
        """
        Initialize the pipeline with StackAdapt and BigQuery clients
        
        Args:
            dataset_id: BigQuery dataset ID
            project_id: GCP project ID (if None, uses default from BigQueryClient)
        """
        self.dataset_id = dataset_id
        
        # Initialize BigQuery client (handles credentials automatically)
        self.bq_client = BigQueryClient()
        
        # Use provided project_id or get from BigQueryClient
        self.project_id = project_id or self.bq_client.project_id
        
        if not self.project_id:
            raise ValueError("Project ID must be specified or available in credentials")
        
        # Initialize StackAdapt client
        self.stackadapt_client = StackAdaptClient()
        
        logger.info(f"Initialized pipeline with project: {self.project_id}, dataset: {self.dataset_id}")
    
    def sync_ads_performance(self, temp_table_name: str = "stackadapt_ads_temp", 
                           use_bulk: bool = True, days_back: int = 30) -> int:
        """
        Fetch ads performance data from StackAdapt and sync to a temporary BigQuery table
        
        Args:
            temp_table_name: Name of the temporary BigQuery table
            use_bulk: Whether to use bulk API call (True) or individual calls (False)
            days_back: Number of days back from today to fetch data (default: 30)
            
        Returns:
            int: Number of records synced
        """
        # Calculate date range
        date_to = datetime.now().strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        logger.info(f"Starting StackAdapt ads performance sync (bulk mode: {use_bulk})...")
        logger.info(f"Date range: {date_from} to {date_to} ({days_back} days back)")
        
        try:
            # Test connection first
            logger.info("Testing StackAdapt API connection...")
            if not self.stackadapt_client.test_connection():
                raise Exception("Failed to connect to StackAdapt API")
            
            logger.info("Connection successful!")
            
            # Fetch all ad insights from StackAdapt
            logger.info("Fetching ad insights from StackAdapt...")
            all_results = self.stackadapt_client.fetch_all_ad_insights(use_bulk=use_bulk, date_from=date_from, date_to=date_to)
            
            if not all_results:
                logger.warning("No data retrieved from StackAdapt")
                return 0
            
            logger.info(f"Retrieved {len(all_results)} result sets from StackAdapt")
            
            # Flatten the nested GraphQL response into a list of records
            records = []
            
            for result in all_results:
                if (result and 'data' in result and 
                    'campaignGroupInsight' in result['data'] and
                    'records' in result['data']['campaignGroupInsight']):
                    
                    edges = result['data']['campaignGroupInsight']['records']['edges']
                    
                    for edge in edges:
                        node = edge['node']
                        
                        # Extract data from the nested structure
                        # Using .get() with default None to preserve NULLs
                        record = {
                            # IDs and names
                            'ad_id': node['attributes']['ad']['id'],
                            'ad_name': node['attributes']['ad']['name'],
                            'date': node['attributes']['date'],
                            'campaign_id': node['attributes']['ad']['campaign']['id'],
                            'campaign_name': node['attributes']['ad']['campaign']['name'],
                            'campaign_group_id': node['attributes']['ad']['campaign']['campaignGroup']['id'],
                            'campaign_group_name': node['attributes']['ad']['campaign']['campaignGroup']['name'],
                            
                            # Goal type information
                            'goalType': node['attributes']['ad']['campaign'].get('goalType'),
                            
                            # Metrics - preserve original metric names from API
                            'clicks': node['metrics'].get('clicks'),
                            'clickConversions': node['metrics'].get('clickConversions'),
                            'engagements': node['metrics'].get('engagements'),
                            'videoStarts': node['metrics'].get('videoStarts'),
                            'videoQ1Playbacks': node['metrics'].get('videoQ1Playbacks'),
                            'videoQ2Playbacks': node['metrics'].get('videoQ2Playbacks'),
                            'videoQ3Playbacks': node['metrics'].get('videoQ3Playbacks'),
                            'videoCompletions': node['metrics'].get('videoCompletions'),
                            'impressions': node['metrics'].get('impressions'),
                            'frequency': node['metrics'].get('frequency'),
                            'cost': node['metrics'].get('cost')  # Keep cost in original format (cents)
                        }
                        
                        records.append(record)
            
            logger.info(f"Processed {len(records)} individual ad performance records")
            
            if not records:
                logger.warning("No individual records found in the response data")
                return 0
            
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Convert date column to datetime
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            # Add metadata columns
            df['_loaded_at'] = pd.Timestamp.now(tz='UTC')
            df['_source'] = 'stackadapt_graphql_api'
            
            # Table ID for pandas-gbq
            table_id = f"{self.dataset_id}.{temp_table_name}"
            
            # Load to BigQuery using pandas-gbq
            logger.info(f"Loading {len(df)} records to {self.project_id}.{table_id}")
            logger.info(f"Columns: {list(df.columns)}")
            
            # Use pandas-gbq to load data
            pandas_gbq.to_gbq(
                df,
                table_id,
                project_id=self.project_id,
                if_exists='replace',  # Replace the temp table
                progress_bar=True
            )
            
            logger.info(f"Successfully loaded {len(df)} performance records to BigQuery temp table")
            
            # Log sample data
            if len(df) > 0:
                sample_cols = ['date', 'ad_name', 'campaign_name', 'impressions', 'clicks', 'cost']
                available_cols = [col for col in sample_cols if col in df.columns]
                logger.info(f"Sample performance data: {df[available_cols].iloc[0].to_dict()}")
            
            # Show summary statistics
            logger.info("\nSummary statistics:")
            if 'cost' in df.columns:
                # Convert cost to numeric, coercing errors to NaN
                df['cost'] = pd.to_numeric(df['cost'], errors='coerce')
                total_cost_cents = df['cost'].sum()
                if pd.notna(total_cost_cents):
                    logger.info(f"Total cost: {total_cost_cents:,.0f} cents (${total_cost_cents/100:,.2f})")
            if 'impressions' in df.columns:
                df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce')
                total_impressions = df['impressions'].sum()
                if pd.notna(total_impressions):
                    logger.info(f"Total impressions: {total_impressions:,.0f}")
            if 'clicks' in df.columns:
                df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce')
                total_clicks = df['clicks'].sum()
                if pd.notna(total_clicks):
                    logger.info(f"Total clicks: {total_clicks:,.0f}")
            if 'clickConversions' in df.columns:
                df['clickConversions'] = pd.to_numeric(df['clickConversions'], errors='coerce')
                total_conversions = df['clickConversions'].sum()
                if pd.notna(total_conversions):
                    logger.info(f"Total conversions: {total_conversions:,.0f}")
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Error syncing StackAdapt ads performance: {str(e)}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get information about a BigQuery table using pandas-gbq
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict with table information
        """
        try:
            # Use INFORMATION_SCHEMA for better compatibility
            query = f"""
            SELECT 
                table_id as table_name,
                creation_time,
                TIMESTAMP_MILLIS(last_modified_time) as last_modified_time,
                row_count,
                size_bytes
            FROM `{self.project_id}.{self.dataset_id}.__TABLES__`
            WHERE table_id = '{table_name}'
            """
            
            result = pandas_gbq.read_gbq(
                query,
                project_id=self.project_id,
                progress_bar_type=None
            )
            
            if not result.empty:
                row = result.iloc[0]
                return {
                    "table_id": f"{self.project_id}.{self.dataset_id}.{table_name}",
                    "created": row['creation_time'],
                    "modified": row['last_modified_time'],
                    "num_rows": row['row_count'],
                    "num_bytes": row['size_bytes']
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            return None
    
    def query_performance_summary(self, table_name: str = "stackadapt_ads_temp") -> pd.DataFrame:
        """
        Query performance summary from the temp table
        
        Args:
            table_name: Name of the table to query
            
        Returns:
            DataFrame with summary data
        """
        query = f"""
        SELECT 
            date,
            campaign_group_name,
            campaign_name,
            COUNT(DISTINCT ad_id) as num_ads,
            SUM(CAST(impressions AS INT64)) as total_impressions,
            SUM(CAST(clicks AS INT64)) as total_clicks,
            SUM(CAST(cost AS FLOAT64)) as total_cost_cents,
            SUM(CAST(cost AS FLOAT64)) / 100.0 as total_cost_dollars,
            SAFE_DIVIDE(SUM(CAST(clicks AS INT64)), SUM(CAST(impressions AS INT64))) as ctr,
            SAFE_DIVIDE(SUM(CAST(cost AS FLOAT64)), SUM(CAST(clicks AS INT64))) / 100.0 as cpc_dollars,
            SAFE_DIVIDE(SUM(CAST(cost AS FLOAT64)), SUM(CAST(impressions AS INT64))) * 10 as cpm_dollars
        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        GROUP BY date, campaign_group_name, campaign_name
        ORDER BY date DESC, total_cost_cents DESC
        LIMIT 20
        """
        
        return pandas_gbq.read_gbq(
            query,
            project_id=self.project_id,
            progress_bar_type=None
        )
    
    def merge_ads_performance(self, temp_table_name: str = "stackadapt_ads_temp", 
                             target_table_name: str = "stackadapt_ads") -> bool:
        """
        Merge ads performance data from temp table into main table with upsert logic
        If main table doesn't exist, create it from temp table data
        
        Args:
            temp_table_name: Name of the temporary table with new data
            target_table_name: Name of the main table to merge into
            
        Returns:
            bool: True if merge was successful, False otherwise
        """
        try:
            # Check if target table exists
            target_table_id = f"{self.project_id}.{self.dataset_id}.{target_table_name}"
            temp_table_id = f"{self.project_id}.{self.dataset_id}.{temp_table_name}"
            
            # Try to get info about target table to see if it exists
            target_exists = self.get_table_info(target_table_name) is not None
            
            if not target_exists:
                logger.info(f"Target table {target_table_name} does not exist. Creating from temp table...")
                
                # Create target table by copying temp table structure and data
                create_query = f"""
                CREATE TABLE `{target_table_id}` AS
                SELECT * FROM `{temp_table_id}`
                """
                
                pandas_gbq.read_gbq(
                    create_query,
                    project_id=self.project_id,
                    progress_bar_type=None
                )
                
                logger.info(f"Successfully created {target_table_name} table with data from {temp_table_name}")
                return True
            
            else:
                logger.info(f"Target table {target_table_name} exists. Performing upsert merge...")
                
                # Perform MERGE operation with upsert logic
                merge_query = f"""
                MERGE `{target_table_id}` AS target
                USING `{temp_table_id}` AS source
                ON target.ad_id = source.ad_id AND target.date = source.date
                WHEN MATCHED THEN
                  UPDATE SET
                    ad_name = source.ad_name,
                    campaign_id = source.campaign_id,
                    campaign_name = source.campaign_name,
                    campaign_group_id = source.campaign_group_id,
                    campaign_group_name = source.campaign_group_name,
                    goalType = source.goalType,
                    clicks = source.clicks,
                    clickConversions = source.clickConversions,
                    engagements = source.engagements,
                    videoStarts = source.videoStarts,
                    videoQ1Playbacks = source.videoQ1Playbacks,
                    videoQ2Playbacks = source.videoQ2Playbacks,
                    videoQ3Playbacks = source.videoQ3Playbacks,
                    videoCompletions = source.videoCompletions,
                    impressions = source.impressions,
                    frequency = source.frequency,
                    cost = source.cost,
                    _loaded_at = source._loaded_at,
                    _source = source._source
                WHEN NOT MATCHED THEN
                  INSERT (
                    ad_id, ad_name, date, campaign_id, campaign_name,
                    campaign_group_id, campaign_group_name, goalType,
                    clicks, clickConversions, engagements, videoStarts,
                    videoQ1Playbacks, videoQ2Playbacks, videoQ3Playbacks,
                    videoCompletions, impressions, frequency, cost,
                    _loaded_at, _source
                  )
                  VALUES (
                    source.ad_id, source.ad_name, source.date, source.campaign_id, source.campaign_name,
                    source.campaign_group_id, source.campaign_group_name, source.goalType,
                    source.clicks, source.clickConversions, source.engagements, source.videoStarts,
                    source.videoQ1Playbacks, source.videoQ2Playbacks, source.videoQ3Playbacks,
                    source.videoCompletions, source.impressions, source.frequency, source.cost,
                    source._loaded_at, source._source
                  )
                """
                
                pandas_gbq.read_gbq(
                    merge_query,
                    project_id=self.project_id,
                    progress_bar_type=None
                )
                
                logger.info(f"Successfully merged data from {temp_table_name} into {target_table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error merging ads performance data: {str(e)}")
            return False


def main():
    """Main execution function"""
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Sync StackAdapt ads data to BigQuery')
    parser.add_argument('--bulk', action='store_true', default=True,
                        help='Use bulk API mode (default: True)')
    parser.add_argument('--dataset', type=str, default='raw_ads',
                        help='BigQuery dataset ID (default: raw_ads)')
    parser.add_argument('--project', type=str, default=None,
                        help='GCP project ID (default: use GOOGLE_CLOUD_PROJECT env var)')
    parser.add_argument('--days-back', type=int, default=30,
                        help='Number of days back from today to fetch data (default: 30)')
    args = parser.parse_args()
    
    logger.info("Starting StackAdapt to BigQuery sync")
    logger.info(f"Using bulk mode: {args.bulk}")
    logger.info(f"Days back: {args.days_back}")
    
    try:
        # Initialize pipeline
        pipeline = StackAdaptToBigQueryPipeline(
            dataset_id=args.dataset,
            project_id=args.project
        )
        
        # Sync ads performance data to temp table
        logger.info("\n" + "="*60)
        logger.info("Syncing StackAdapt ads performance data...")
        count = pipeline.sync_ads_performance(use_bulk=args.bulk, days_back=args.days_back)
        logger.info(f"Synced {count} performance records to temp table")
        
        if count > 0:
            # Merge temp data into main table
            logger.info("\n" + "="*60)
            logger.info("Merging temp data into main stackadapt_ads table...")
            merge_success = pipeline.merge_ads_performance()
            if merge_success:
                logger.info("Successfully merged data into main table")
                
                # Get main table info
                main_table_info = pipeline.get_table_info("stackadapt_ads")
                if main_table_info:
                    logger.info(f"Main table info: {json.dumps(main_table_info, indent=2, default=str)}")
            else:
                logger.error("Failed to merge data into main table")
            
            # Get table info
            logger.info("\n" + "="*60)
            logger.info("Temp table information:")
            table_info = pipeline.get_table_info("stackadapt_ads_temp")
            if table_info:
                logger.info(f"Table info: {json.dumps(table_info, indent=2, default=str)}")
            
            # Query summary data
            logger.info("\n" + "="*60)
            logger.info("Performance summary by campaign:")
            summary_df = pipeline.query_performance_summary()
            if not summary_df.empty:
                logger.info(f"\n{summary_df.to_string()}")
            
            # Show date range of data
            logger.info("\n" + "="*60)
            logger.info("Date range of data in temp table:")
            date_range_query = f"""
            SELECT 
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                COUNT(DISTINCT date) as days_of_data,
                COUNT(*) as total_rows,
                COUNT(DISTINCT campaign_group_id) as num_campaign_groups,
                COUNT(DISTINCT campaign_id) as num_campaigns,
                COUNT(DISTINCT ad_id) as num_ads
            FROM `{pipeline.project_id}.{pipeline.dataset_id}.stackadapt_ads_temp`
            """
            date_range = pandas_gbq.read_gbq(
                date_range_query,
                project_id=pipeline.project_id,
                progress_bar_type=None
            )
            logger.info(f"\n{date_range.to_string()}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
