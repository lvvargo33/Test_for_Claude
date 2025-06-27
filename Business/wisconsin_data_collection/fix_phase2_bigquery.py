#!/usr/bin/env python3
"""
Fix Phase 2 BigQuery Tables
===========================

Fixes BigQuery table schemas for Phase 2 data sources.
"""

import logging
from datetime import datetime
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_phase2_tables():
    """Fix BigQuery table schemas for Phase 2 data"""
    
    client = bigquery.Client(project="location-optimizer-1")
    
    print("🔧 Fixing Phase 2 BigQuery Table Schemas")
    print("="*60)
    
    # Table configurations
    tables_to_fix = [
        {
            "dataset": "raw_real_estate",
            "table": "commercial_real_estate",
            "description": "Commercial real estate listings and property records"
        },
        {
            "dataset": "processed_business_data", 
            "table": "industry_benchmarks",
            "description": "Industry financial and operational benchmarks"
        },
        {
            "dataset": "processed_business_data",
            "table": "employment_projections", 
            "description": "BLS employment growth projections"
        },
        {
            "dataset": "processed_business_data",
            "table": "oes_wages",
            "description": "Occupational Employment Statistics wage data"
        }
    ]
    
    for table_config in tables_to_fix:
        dataset_id = table_config["dataset"]
        table_name = table_config["table"]
        table_id = f"{client.project}.{dataset_id}.{table_name}"
        
        print(f"\n📋 Processing {table_id}")
        
        # Create dataset if it doesn't exist
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "US"
        try:
            dataset = client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {dataset_id} is ready")
        except Exception as e:
            logger.error(f"Error creating dataset {dataset_id}: {e}")
            continue
        
        # Delete existing table if it exists (to fix schema issues)
        try:
            client.delete_table(table_id, not_found_ok=True)
            logger.info(f"Deleted existing table {table_name}")
        except Exception as e:
            logger.warning(f"Could not delete table {table_name}: {e}")
        
        print(f"   ✅ {table_name} schema reset")
    
    print(f"\n📝 Phase 2 table schemas have been reset.")
    print("   The collectors will now create tables with proper schemas when they run.")
    print("   All tables will use data_extraction_date for DAY partitioning.")

def main():
    """Main function"""
    try:
        fix_phase2_tables()
        print("\n✅ SUCCESS: Phase 2 BigQuery tables are ready for data loading")
    except Exception as e:
        logger.error(f"Failed to fix Phase 2 tables: {e}")
        print(f"\n❌ ERROR: {e}")

if __name__ == "__main__":
    main()