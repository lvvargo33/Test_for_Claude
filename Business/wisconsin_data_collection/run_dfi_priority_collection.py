#!/usr/bin/env python3
"""
Run priority DFI collection - focus on key business types to load data quickly
"""

import os
import sys
sys.path.append('.')

from dfi_collector import DFIBusinessCollector
from google.cloud import bigquery
import logging
from datetime import datetime, timedelta

def run_priority_dfi_collection():
    print('🎯 Running Priority DFI Collection')
    print('=' * 50)
    
    # Set up logging
    logging.basicConfig(level=logging.WARNING)  # Reduce log noise
    
    try:
        # Initialize BigQuery client
        print('📊 Initializing BigQuery client...')
        client = bigquery.Client()
        
        # Initialize DFI collector
        collector = DFIBusinessCollector()
        
        # Priority business types (most important for user's consulting business)
        priority_keywords = ['RESTAURANT', 'SALON', 'FITNESS', 'AUTO', 'RETAIL']
        
        print(f'🔍 Collecting priority Wisconsin businesses (last 90 days)...')
        print(f'   Focus keywords: {", ".join(priority_keywords)}')
        
        all_businesses = []
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')
        
        # Search each priority keyword
        for i, keyword in enumerate(priority_keywords, 1):
            print(f'   {i}/{len(priority_keywords)} Searching {keyword} businesses...')
            
            businesses = collector.search_registrations_by_keyword(keyword, start_date_str, end_date_str)
            all_businesses.extend(businesses)
            
            print(f'      Found: {len(businesses)} businesses')
        
        # Remove duplicates
        unique_businesses = []
        seen_ids = set()
        
        for business in all_businesses:
            if business.business_id not in seen_ids:
                unique_businesses.append(business)
                seen_ids.add(business.business_id)
        
        print(f'\n📊 Collection Summary:')
        print(f'   Total found: {len(all_businesses)} businesses')
        print(f'   Unique businesses: {len(unique_businesses)} (after deduplication)')
        
        if not unique_businesses:
            print('ℹ️  No businesses found to load')
            return False
        
        # Group by business type
        type_counts = {}
        for business in unique_businesses:
            btype = business.business_type or 'Unclassified'
            type_counts[btype] = type_counts.get(btype, 0) + 1
        
        print(f'\n📋 Business Types:')
        for btype, count in sorted(type_counts.items()):
            print(f'   • {btype}: {count} businesses')
        
        # Prepare for BigQuery
        print(f'\n💾 Loading {len(unique_businesses)} businesses into BigQuery...')
        
        table_id = "location-optimizer-1.raw_business_data.dfi_business_registrations"
        
        rows_to_insert = []
        for business in unique_businesses:
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
        
        # Insert into BigQuery
        errors = client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            print(f'❌ BigQuery errors:')
            for error in errors:
                print(f'   • {error}')
            return False
        
        print(f'✅ Successfully loaded {len(rows_to_insert)} businesses!')
        
        # Verify data
        print(f'\n🔍 Verifying loaded data...')
        
        verify_query = f"""
        SELECT 
            business_type,
            COUNT(*) as count,
            MIN(registration_date) as earliest,
            MAX(registration_date) as latest
        FROM `{table_id}`
        WHERE source = 'DFI' 
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTES)
        GROUP BY business_type
        ORDER BY count DESC
        """
        
        results = client.query(verify_query).result()
        
        total_verified = 0
        for row in results:
            print(f'   ✓ {row.business_type}: {row.count} businesses ({row.earliest} to {row.latest})')
            total_verified += row.count
        
        # Show recent examples
        print(f'\n🎯 Recent Wisconsin Business Examples:')
        
        sample_query = f"""
        SELECT 
            business_name,
            business_type,
            registration_date,
            status
        FROM `{table_id}`
        WHERE source = 'DFI'
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTES)
        ORDER BY registration_date DESC
        LIMIT 8
        """
        
        sample_results = client.query(sample_query).result()
        
        for i, row in enumerate(sample_results, 1):
            print(f'   {i}. {row.business_name}')
            print(f'      {row.business_type} | {row.registration_date} | {row.status}')
        
        print(f'\n🎉 Successfully loaded {total_verified} Wisconsin DFI businesses!')
        print(f'📊 Real data now available in BigQuery for prospect analysis')
        
        return True
        
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_priority_dfi_collection()
    if success:
        print('\n✅ Priority DFI collection completed!')
        print('🚀 Wisconsin business data ready for outreach analysis')
    else:
        print('\n❌ Collection failed')