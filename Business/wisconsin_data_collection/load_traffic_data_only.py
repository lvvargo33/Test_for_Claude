#!/usr/bin/env python3
"""
Load Traffic Data to BigQuery
=============================

Loads only the traffic data that was successfully collected.
"""

import logging
from datetime import datetime
from google.cloud import bigquery

from traffic_data_collector import WisconsinTrafficDataCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Load traffic data to BigQuery"""
    
    print("🚦 Loading Wisconsin Traffic Data to BigQuery")
    print("="*60)
    
    # Initialize collector
    traffic_collector = WisconsinTrafficDataCollector()
    
    # Run traffic collection (this method handles BigQuery loading internally)
    logger.info("Collecting traffic data...")
    summary = traffic_collector.run_traffic_collection(max_records=10000)
    
    if not summary.get('success'):
        logger.error(f"Traffic collection failed: {summary.get('errors', [])}")
        print(f"\n❌ Traffic collection failed")
        print(f"   Records collected: {summary.get('total_records', 0)}")
        print(f"   Highway records: {summary.get('highway_records', 0)}")
        print(f"   Errors: {summary.get('errors', [])}")
        return
    
    print(f"\n✅ SUCCESS: Traffic data collection and loading complete!")
    print(f"   Total records: {summary['total_records']:,}")
    print(f"   Highway records: {summary['highway_records']:,}")
    print(f"   City records: {summary['city_records']:,}")
    print(f"   Processing time: {summary['processing_time_seconds']:.2f} seconds")
    
    # Verify data in BigQuery
    client = bigquery.Client(project="location-optimizer-1")
    table_id = "location-optimizer-1.raw_traffic.traffic_counts"
    
    try:
        table = client.get_table(table_id)
        print(f"\n📊 BigQuery Table Status:")
        print(f"   Table: {table_id}")
        print(f"   Total rows: {table.num_rows:,}")
        print(f"   Size: {table.num_bytes / 1024 / 1024:.2f} MB")
        
        # Show sample of loaded data
        query = f"""
        SELECT 
            highway_name,
            county,
            aadt,
            aadt_year,
            functional_class
        FROM `{table_id}`
        ORDER BY aadt DESC
        LIMIT 5
        """
        
        print("\n📊 Top 5 Traffic Locations by AADT:")
        for row in client.query(query):
            print(f"   {row.highway_name} ({row.county}): {row.aadt:,} AADT ({row.aadt_year})")
            
    except Exception as e:
        logger.warning(f"Could not verify BigQuery table: {e}")

if __name__ == "__main__":
    main()