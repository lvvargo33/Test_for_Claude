#!/usr/bin/env python3
"""
Quick DFI data load - just get RESTAURANT data into BigQuery to verify the process works
"""

import os
import sys
sys.path.append('.')

from dfi_collector import DFIBusinessCollector
from google.cloud import bigquery
from datetime import datetime, timedelta

def quick_dfi_load():
    print('⚡ Quick DFI Data Load Test')
    print('=' * 40)
    
    try:
        client = bigquery.Client()
        collector = DFIBusinessCollector()
        
        # Get just restaurant data from last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')
        
        print(f'🍕 Collecting RESTAURANT businesses ({start_date_str} to {end_date_str})...')
        
        # Disable duplicate checking to avoid BigQuery dependency during collection
        collector._check_for_duplicates = lambda name, date: False
        
        businesses = collector.search_registrations_by_keyword('RESTAURANT', start_date_str, end_date_str)
        
        print(f'📊 Found {len(businesses)} restaurant businesses')
        
        if not businesses:
            print('ℹ️  No businesses found')
            return False
        
        # Show what we found
        print(f'\n🎯 Sample businesses to load:')
        for i, business in enumerate(businesses[:3]):
            print(f'  {i+1}. {business.business_name}')
            print(f'     Date: {business.registration_date} | Status: {business.status}')
        
        # Prepare for BigQuery (match the existing schema)
        table_id = "location-optimizer-1.raw_business_data.dfi_business_registrations"
        
        rows_to_insert = []
        for business in businesses:
            # Parse registration date and convert to string for JSON
            try:
                reg_date = datetime.strptime(business.registration_date, '%m/%d/%Y').date()
                reg_date_str = reg_date.isoformat()
            except:
                reg_date_str = None
            
            row = {
                'business_id': business.business_id,
                'business_name': business.business_name,
                'entity_type': business.entity_type,
                'registration_date': reg_date_str,
                'status': business.status,
                'business_type': business.business_type,
                'agent_name': business.agent_name,
                'business_address': business.business_address,
                'city': business.city,
                'state': business.state,
                'zip_code': business.zip_code,
                'county': business.county,
                'naics_code': business.naics_code,
                'source': business.source,
                'data_extraction_date': datetime.utcnow().isoformat(),
                'is_target_business': True
            }
            rows_to_insert.append(row)
        
        print(f'\n💾 Loading {len(rows_to_insert)} businesses into BigQuery...')
        
        # Insert into BigQuery
        errors = client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            print(f'❌ BigQuery errors:')
            for error in errors:
                print(f'   • {error}')
            return False
        
        print(f'✅ Successfully loaded {len(rows_to_insert)} restaurant businesses!')
        
        # Verify the data
        print(f'\n🔍 Verifying data...')
        
        verify_query = f"""
        SELECT 
            COUNT(*) as total,
            business_type,
            MIN(registration_date) as earliest,
            MAX(registration_date) as latest
        FROM `{table_id}`
        WHERE source = 'DFI'
        GROUP BY business_type
        """
        
        results = client.query(verify_query).result()
        
        for row in results:
            print(f'✓ {row.business_type}: {row.total} businesses ({row.earliest} to {row.latest})')
        
        # Show sample loaded data
        sample_query = f"""
        SELECT 
            business_name,
            registration_date,
            status,
            business_id
        FROM `{table_id}`
        WHERE source = 'DFI'
        ORDER BY registration_date DESC
        LIMIT 5
        """
        
        sample_results = client.query(sample_query).result()
        
        print(f'\n🎯 Sample loaded businesses:')
        for i, row in enumerate(sample_results, 1):
            print(f'  {i}. {row.business_name}')
            print(f'     Date: {row.registration_date} | Status: {row.status}')
            print(f'     ID: {row.business_id}')
            print()
        
        print(f'🎉 DFI data successfully loaded into BigQuery!')
        return True
        
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = quick_dfi_load()
    if success:
        print('\n✅ Quick DFI load successful!')
        print('📊 Real Wisconsin business data now in BigQuery')
    else:
        print('\n❌ Load failed')