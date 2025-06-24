#!/usr/bin/env python3
"""
Run full DFI collection process and load real data into BigQuery
"""

import os
import sys
sys.path.append('.')

from dfi_collector import DFIBusinessCollector
from google.cloud import bigquery
import logging
from datetime import datetime

def run_full_dfi_collection():
    print('🔄 Running Full DFI Collection Process')
    print('=' * 50)
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    
    try:
        # Initialize BigQuery client
        print('📊 Initializing BigQuery client...')
        client = bigquery.Client()
        
        # Initialize DFI collector
        print('🏢 Initializing DFI collector...')
        collector = DFIBusinessCollector()
        
        # Collect recent DFI registrations (last 90 days)
        print('🔍 Collecting recent Wisconsin business registrations...')
        print('   Searching for target business types in last 90 days')
        
        businesses = collector.collect_recent_registrations(days_back=90)
        
        print(f'\n📊 Collection Results:')
        print(f'   Total businesses found: {len(businesses)}')
        
        if not businesses:
            print('ℹ️  No businesses found to load')
            return False
        
        # Group by business type for summary
        type_counts = {}
        for business in businesses:
            btype = business.business_type or 'Unclassified'
            type_counts[btype] = type_counts.get(btype, 0) + 1
        
        print(f'\n📋 Business Types Found:')
        for btype, count in sorted(type_counts.items()):
            print(f'   • {btype}: {count} businesses')
        
        # Prepare data for BigQuery
        print(f'\n💾 Preparing data for BigQuery...')
        
        table_id = "location-optimizer-1.raw_business_data.dfi_business_registrations"
        
        # Convert to BigQuery format
        rows_to_insert = []
        for business in businesses:
            # Parse registration date
            try:
                reg_date = datetime.strptime(business.registration_date, '%m/%d/%Y').date()
            except:
                reg_date = None
            
            row = {
                'business_name': business.business_name,
                'entity_type': business.entity_type,
                'registration_date': reg_date,
                'status': business.status,
                'business_id': business.business_id,
                'agent_name': business.agent_name,
                'business_address': business.business_address,
                'city': business.city,
                'state': business.state,
                'zip_code': business.zip_code,
                'county': business.county,
                'naics_code': business.naics_code,
                'business_type': business.business_type,
                'source': business.source,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            rows_to_insert.append(row)
        
        print(f'   Prepared {len(rows_to_insert)} rows for insertion')
        
        # Insert into BigQuery
        print(f'📤 Loading data into BigQuery...')
        print(f'   Table: {table_id}')
        
        errors = client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            print(f'❌ BigQuery insertion errors:')
            for error in errors:
                print(f'   • {error}')
            return False
        else:
            print(f'✅ Successfully loaded {len(rows_to_insert)} businesses into BigQuery!')
        
        # Verify the data was loaded
        print(f'\n🔍 Verifying data in BigQuery...')
        
        query = f"""
        SELECT 
            business_type,
            COUNT(*) as count,
            MIN(registration_date) as earliest_date,
            MAX(registration_date) as latest_date
        FROM `{table_id}`
        WHERE source = 'DFI'
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        GROUP BY business_type
        ORDER BY count DESC
        """
        
        results = client.query(query).result()
        
        print(f'📊 Data verification results:')
        total_loaded = 0
        for row in results:
            print(f'   • {row.business_type}: {row.count} businesses')
            print(f'     Date range: {row.earliest_date} to {row.latest_date}')
            total_loaded += row.count
        
        print(f'\n🎉 Successfully loaded {total_loaded} Wisconsin DFI businesses into BigQuery!')
        print(f'📈 Data is now available for business opportunity analysis')
        
        # Show sample recent businesses
        print(f'\n🎯 Sample Recent Target Businesses:')
        
        sample_query = f"""
        SELECT 
            business_name,
            business_type,
            registration_date,
            status,
            business_id
        FROM `{table_id}`
        WHERE source = 'DFI'
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        ORDER BY registration_date DESC
        LIMIT 10
        """
        
        sample_results = client.query(sample_query).result()
        
        for i, row in enumerate(sample_results, 1):
            print(f'   {i}. {row.business_name}')
            print(f'      Type: {row.business_type} | Date: {row.registration_date}')
            print(f'      Status: {row.status} | ID: {row.business_id}')
            print()
        
        return True
        
    except Exception as e:
        print(f'❌ Error in full DFI collection: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_full_dfi_collection()
    if success:
        print('\n✅ Full DFI collection completed successfully!')
        print('🚀 Real Wisconsin business data now available in BigQuery')
    else:
        print('\n❌ DFI collection failed - check logs for details')