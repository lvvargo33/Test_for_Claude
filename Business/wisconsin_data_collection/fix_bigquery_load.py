#!/usr/bin/env python3
"""
Fix BigQuery Load - Match Existing Schema
========================================

Load BEA data to BigQuery matching the existing table schema.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548.json'

def check_existing_table_schema():
    """Check the existing table schema"""
    
    client = bigquery.Client(project="location-optimizer-1")
    table_id = "location-optimizer-1.raw_business_data.consumer_spending"
    
    try:
        table = client.get_table(table_id)
        
        print("Existing table schema:")
        print("-" * 50)
        for field in table.schema:
            print(f"{field.name:30} {field.field_type:15} {field.mode}")
        
        print(f"\nPartitioning: {table.time_partitioning}")
        print(f"Clustering: {table.clustering_fields}")
        print(f"Rows: {table.num_rows:,}")
        
        return table
        
    except Exception as e:
        logger.error(f"Error checking table: {e}")
        return None

def transform_data_to_match_schema():
    """Transform our collected data to match the existing schema"""
    
    # Load our collected data
    df = pd.read_csv('wisconsin_consumer_spending_comprehensive.csv')
    
    print(f"Original data shape: {df.shape}")
    print(f"Original columns: {list(df.columns)}")
    
    # Transform to match existing schema (based on error message)
    transformed_records = []
    
    for _, row in df.iterrows():
        year = int(row['data_year'])
        
        # Create records for each spending category
        categories = {
            'total_pce': 'Total PCE',
            'goods_total': 'Goods Total',
            'goods_durable': 'Durable Goods',
            'goods_nondurable': 'Nondurable Goods',
            'services_total': 'Services Total'
        }
        
        for category_col, category_name in categories.items():
            if category_col in row and pd.notna(row[category_col]):
                
                # Calculate per capita spending
                population = row.get('population', 5900000)  # Default to ~5.9M
                spending_amount = row[category_col] * 1_000_000  # Convert millions to dollars
                per_capita = spending_amount / population if population > 0 else 0
                
                record = {
                    'state': 'WI',
                    'state_fips': '55',
                    'year': year,
                    'spending_category': category_col,
                    'spending_amount': spending_amount,
                    'per_capita_spending': per_capita,
                    'data_source': 'BEA_API_Real',
                    'api_dataset': 'Regional_SAPCE1',
                    'geo_name': 'Wisconsin',
                    'collection_date': datetime.now(),
                    'data_extraction_date': datetime.now()
                }
                
                transformed_records.append(record)
    
    # Create transformed DataFrame
    transformed_df = pd.DataFrame(transformed_records)
    
    print(f"Transformed data shape: {transformed_df.shape}")
    print(f"Transformed columns: {list(transformed_df.columns)}")
    
    return transformed_df

def load_with_correct_schema(df):
    """Load data with the correct schema to match existing table"""
    
    client = bigquery.Client(project="location-optimizer-1")
    table_id = "location-optimizer-1.raw_business_data.consumer_spending"
    
    try:
        # Use WRITE_APPEND to add to existing data
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # Append to existing data
            autodetect=False,  # Don't auto-detect schema
            # Match the existing partitioning and clustering
            # Based on error: partitioning(data_extraction_date) clustering(state,year,spending_category)
        )
        
        logger.info(f"Loading {len(df)} records to {table_id}...")
        
        # Load data
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        # Check results
        table = client.get_table(table_id)
        logger.info(f"Successfully loaded data to {table_id}")
        logger.info(f"Total rows in table: {table.num_rows:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading to BigQuery: {e}")
        return False

def verify_loaded_data():
    """Verify the loaded data"""
    
    client = bigquery.Client(project="location-optimizer-1")
    
    # Query to check our loaded data
    query = """
    SELECT 
        year,
        spending_category,
        ROUND(spending_amount / 1000000000, 2) as spending_billions,
        ROUND(per_capita_spending, 0) as per_capita,
        data_source
    FROM `location-optimizer-1.raw_business_data.consumer_spending`
    WHERE data_source = 'BEA_API_Real'
      AND spending_category = 'total_pce'
      AND year >= 2015
    ORDER BY year DESC
    """
    
    print("\n📊 Verification - Wisconsin Total PCE (BEA Real Data):")
    print("Year | Amount ($B) | Per Capita | Source")
    print("-" * 50)
    
    try:
        for row in client.query(query):
            print(f"{row.year} | ${row.spending_billions:>10.2f} | ${row.per_capita:>9,.0f} | {row.data_source}")
    except Exception as e:
        logger.error(f"Error running verification query: {e}")

def main():
    """Main function"""
    
    print("🔧 Fixing BigQuery Load - Matching Existing Schema")
    print("="*60)
    
    # Check existing table
    print("\n1. Checking existing table schema...")
    existing_table = check_existing_table_schema()
    
    if not existing_table:
        print("❌ Could not access existing table")
        return
    
    # Transform our data
    print("\n2. Transforming data to match schema...")
    transformed_df = transform_data_to_match_schema()
    
    # Load to BigQuery
    print("\n3. Loading to BigQuery...")
    success = load_with_correct_schema(transformed_df)
    
    if success:
        print("✅ Successfully loaded data!")
        
        # Verify
        print("\n4. Verifying loaded data...")
        verify_loaded_data()
        
    else:
        print("❌ Failed to load data")
        
        # Save as backup
        output_file = 'wisconsin_bea_transformed.csv'
        transformed_df.to_csv(output_file, index=False)
        print(f"💾 Transformed data saved to {output_file}")

if __name__ == "__main__":
    main()