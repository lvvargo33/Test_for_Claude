#!/usr/bin/env python3
"""
Fix Phase 1 Data Collection Issues
==================================

Addresses:
1. Traffic data - BigQuery partitioning field type
2. Zoning data - Network connectivity 
3. Consumer spending - BEA API key setup
"""

import os
import logging
from datetime import datetime
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_traffic_table_schema():
    """Fix the traffic table schema to support DAY partitioning"""
    client = bigquery.Client(project="location-optimizer-1")
    
    # Create dataset if it doesn't exist
    dataset_id = "raw_traffic"
    dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset.location = "US"
    
    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {dataset_id} is ready")
    except Exception as e:
        logger.error(f"Error creating dataset: {e}")
        return False
    
    # Define proper schema with TIMESTAMP field for partitioning
    table_id = f"{client.project}.{dataset_id}.traffic_counts"
    
    schema = [
        bigquery.SchemaField("traffic_count_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("county", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("highway_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("highway_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("location_description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("aadt", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("aadt_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("functional_class", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("lane_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("median_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("access_control", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("truck_percentage", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("peak_hour_factor", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("directional_split", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("data_source", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collection_method", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("data_quality_score", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("collection_date", "TIMESTAMP", mode="REQUIRED"),  # For partitioning
        bigquery.SchemaField("data_extraction_date", "TIMESTAMP", mode="REQUIRED"),  # Alternative partitioning field
    ]
    
    # Create table with partitioning
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="data_extraction_date"  # Use this field for partitioning
    )
    
    try:
        # Delete existing table if it exists
        client.delete_table(table_id, not_found_ok=True)
        logger.info(f"Deleted existing table {table_id}")
        
        # Create new table
        table = client.create_table(table)
        logger.info(f"Created table {table_id} with proper schema")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False

def setup_bea_api_key():
    """Setup BEA API key environment variable"""
    # Check if already set
    if os.environ.get("BEA_API_KEY"):
        logger.info("BEA API key already set")
        return True
    
    # Common demo/test key for BEA (register at https://apps.bea.gov/api/signup/)
    # This is a placeholder - user should get their own key
    demo_key = "YOUR-BEA-API-KEY-HERE"
    
    logger.warning("BEA API key not found. Please register at https://apps.bea.gov/api/signup/")
    logger.info("Set your key with: export BEA_API_KEY='your-key-here'")
    logger.info("For now, the collector will use demo data mode")
    
    return False

def test_network_connectivity():
    """Test network connectivity to data sources"""
    import requests
    
    test_urls = {
        "WisDOT API": "https://services1.arcgis.com/WzFsmainVTuD5KML/arcgis/rest/services/Traffic_Counts/FeatureServer/0",
        "Dane County GIS": "https://gis-portal.data.wi.gov/datasets",  # Alternative URL
        "BEA API": "https://apps.bea.gov/api/data"
    }
    
    print("\n" + "="*60)
    print("NETWORK CONNECTIVITY TEST")
    print("="*60)
    
    for name, url in test_urls.items():
        try:
            response = requests.head(url, timeout=5)
            if response.status_code < 500:
                print(f"✅ {name}: Connected")
            else:
                print(f"⚠️  {name}: Server error ({response.status_code})")
        except Exception as e:
            print(f"❌ {name}: Connection failed - {str(e)[:50]}")
    
    print("="*60)

def main():
    """Main function to fix Phase 1 issues"""
    print("🔧 Fixing Phase 1 Data Collection Issues")
    print("="*60)
    
    # 1. Fix traffic table schema
    print("\n1. Fixing Traffic Table Schema...")
    if fix_traffic_table_schema():
        print("   ✅ Traffic table schema fixed")
    else:
        print("   ❌ Failed to fix traffic table schema")
    
    # 2. Setup BEA API key
    print("\n2. Checking BEA API Key...")
    if setup_bea_api_key():
        print("   ✅ BEA API key configured")
    else:
        print("   ⚠️  BEA API key needs to be configured")
    
    # 3. Test network connectivity
    print("\n3. Testing Network Connectivity...")
    test_network_connectivity()
    
    print("\n📝 NEXT STEPS:")
    print("1. Get a BEA API key from https://apps.bea.gov/api/signup/")
    print("2. Set it with: export BEA_API_KEY='your-key-here'")
    print("3. For zoning data, we'll use alternative data sources")
    print("4. Re-run: python run_phase1_data_collection.py")

if __name__ == "__main__":
    main()