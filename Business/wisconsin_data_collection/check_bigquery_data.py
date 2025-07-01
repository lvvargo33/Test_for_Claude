#!/usr/bin/env python3
"""
Check BigQuery Data Inventory
============================

Check what data we currently have in BigQuery for the deliverability assessment.
"""

import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548.json'

def check_bigquery_inventory():
    """Check current BigQuery data inventory"""
    
    client = bigquery.Client(project="location-optimizer-1")
    
    print("📊 Current BigQuery Data Inventory")
    print("="*50)
    
    # Check datasets
    datasets = list(client.list_datasets())
    
    for dataset in datasets:
        dataset_id = dataset.dataset_id
        print(f"\n📁 Dataset: {dataset_id}")
        print("-" * 40)
        
        # List tables in dataset
        try:
            tables = list(client.list_tables(dataset_id))
            
            for table in tables:
                table_ref = client.get_table(table.reference)
                
                print(f"📋 Table: {table.table_id}")
                print(f"   Rows: {table_ref.num_rows:,}")
                print(f"   Size: {table_ref.num_bytes:,} bytes")
                print(f"   Created: {table_ref.created}")
                print(f"   Schema: {len(table_ref.schema)} columns")
                
                # Show sample of data for key tables
                if table.table_id in ['consumer_spending', 'google_places_data']:
                    print(f"   Sample columns: {[field.name for field in table_ref.schema[:5]]}")
                
                print()
                
        except Exception as e:
            print(f"   Error accessing tables: {e}")

def check_data_coverage():
    """Check geographic and temporal coverage of our data"""
    
    client = bigquery.Client(project="location-optimizer-1")
    
    print("\n🗺️  Data Coverage Analysis")
    print("="*50)
    
    # Check consumer spending coverage
    try:
        query = """
        SELECT 
            MIN(year) as earliest_year,
            MAX(year) as latest_year,
            COUNT(DISTINCT year) as years_covered,
            COUNT(DISTINCT spending_category) as categories,
            data_source
        FROM `location-optimizer-1.raw_business_data.consumer_spending`
        GROUP BY data_source
        """
        
        print("\n💰 Consumer Spending Data:")
        for row in client.query(query):
            print(f"   Source: {row.data_source}")
            print(f"   Years: {row.earliest_year}-{row.latest_year} ({row.years_covered} years)")
            print(f"   Categories: {row.categories}")
            
    except Exception as e:
        print(f"   Consumer spending data not available: {e}")
    
    # Check Google Places coverage
    try:
        query = """
        SELECT 
            COUNT(*) as total_businesses,
            COUNT(DISTINCT county) as counties_covered,
            MIN(data_collection_date) as earliest_collection,
            MAX(data_collection_date) as latest_collection
        FROM `location-optimizer-1.raw_business_data.google_places_data`
        """
        
        print("\n🏢 Google Places Data:")
        for row in client.query(query):
            print(f"   Total businesses: {row.total_businesses:,}")
            print(f"   Counties covered: {row.counties_covered}")
            print(f"   Collection period: {row.earliest_collection} to {row.latest_collection}")
            
    except Exception as e:
        print(f"   Google Places data not available: {e}")

if __name__ == "__main__":
    check_bigquery_inventory()
    check_data_coverage()