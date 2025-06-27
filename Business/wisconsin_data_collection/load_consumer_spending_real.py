#!/usr/bin/env python3
"""
Load Real Consumer Spending Data from BEA
=========================================

Uses the BEA API to load actual consumer spending data.
"""

import os
import logging
from datetime import datetime
from google.cloud import bigquery

# Set the BEA API key
os.environ['BEA_API_KEY'] = '1988DB31-BD6F-4482-A53F-F82AA2BE2E23'

from consumer_spending_collector import BEAConsumerSpendingCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Load real consumer spending data to BigQuery"""
    
    print("💰 Loading Real Consumer Spending Data from BEA to BigQuery")
    print("="*60)
    
    # Initialize collector with API key
    collector = BEAConsumerSpendingCollector()
    
    # Run collection for recent years
    logger.info("Collecting consumer spending data...")
    summary = collector.run_consumer_spending_collection(
        years=[2019, 2020, 2021, 2022, 2023]  # Get 5 years of data
    )
    
    if summary.get('success'):
        print(f"\n✅ SUCCESS: Consumer spending data collection complete!")
        print(f"   Total records: {summary['total_records']:,}")
        print(f"   State records: {summary['state_records']:,}")
        print(f"   County records: {summary['county_records']:,}")
        print(f"   Years collected: {summary['years_collected']}")
        print(f"   Processing time: {summary['processing_time_seconds']:.2f} seconds")
        
        # Verify data in BigQuery
        client = bigquery.Client(project="location-optimizer-1")
        table_id = "location-optimizer-1.raw_business_data.consumer_spending"
        
        try:
            table = client.get_table(table_id)
            print(f"\n📊 BigQuery Table Status:")
            print(f"   Table: {table_id}")
            print(f"   Total rows: {table.num_rows:,}")
            print(f"   Size: {table.num_bytes / 1024 / 1024:.2f} MB")
            
            # Show sample of loaded data
            query = f"""
            SELECT 
                state,
                year,
                spending_category,
                ROUND(spending_amount / 1000000000, 2) as spending_billions,
                ROUND(per_capita_spending, 0) as per_capita,
                ROUND(year_over_year_growth * 100, 1) as yoy_growth_pct
            FROM `{table_id}`
            WHERE spending_category IN ('total_pce', 'food_beverages', 'restaurants_hotels')
            ORDER BY year DESC, spending_category
            LIMIT 9
            """
            
            print("\n📊 Sample Consumer Spending Data:")
            print("   Category | Year | Amount ($B) | Per Capita | YoY Growth")
            print("   " + "-"*55)
            for row in client.query(query):
                category = row.spending_category.replace('_', ' ').title()
                print(f"   {category:<20} | {row.year} | ${row.spending_billions:>6.1f}B | ${row.per_capita:>6,.0f} | {row.yoy_growth_pct:>5.1f}%")
                
        except Exception as e:
            logger.warning(f"Could not verify BigQuery table: {e}")
    else:
        print(f"\n❌ Consumer spending collection failed")
        print(f"   Records collected: {summary.get('total_records', 0)}")
        print(f"   Errors: {summary.get('errors', [])}")

if __name__ == "__main__":
    main()