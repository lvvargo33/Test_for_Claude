#!/usr/bin/env python3
"""
Weekly DFI Collection Script
Run every Monday morning to collect new Wisconsin business registrations
"""

import os
import sys
sys.path.append('.')

from dfi_collector import DFIBusinessCollector
from google.cloud import bigquery
import logging
from datetime import datetime, timedelta

def weekly_dfi_collection():
    print('📅 Weekly DFI Collection - Monday Morning Update')
    print('=' * 60)
    print(f'🕒 Run time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/workspaces/Test_for_Claude/Business/wisconsin_data_collection/logs/weekly_dfi_collection.log'),
            logging.StreamHandler()
        ]
    )
    
    try:
        # Initialize clients
        print('🔧 Initializing BigQuery and DFI collector...')
        client = bigquery.Client()
        collector = DFIBusinessCollector()
        
        # Calculate date range: Last 7 days (to catch any new registrations)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')
        
        print(f'📅 Collection period: {start_date_str} to {end_date_str}')
        
        # Priority keywords for weekly collection (focus on most important business types)
        weekly_keywords = [
            'RESTAURANT', 'PIZZA', 'CAFE', 'BAR', 'FOOD',
            'SALON', 'SPA', 'BEAUTY', 'HAIR', 
            'FITNESS', 'GYM', 'YOGA',
            'AUTO', 'REPAIR',
            'RETAIL', 'STORE', 'SHOP',
            'HOTEL', 'MOTEL',
            'CLEANING', 'LAUNDRY'
        ]
        
        print(f'🔍 Searching {len(weekly_keywords)} business types...')
        
        all_businesses = []
        keyword_results = {}
        
        # Disable duplicate checking during collection (we'll handle it in BigQuery)
        collector._check_for_duplicates = lambda name, date: False
        
        # Collect data for each keyword
        for i, keyword in enumerate(weekly_keywords, 1):
            try:
                print(f'   {i:2d}/{len(weekly_keywords)} {keyword}...', end=' ')
                
                businesses = collector.search_registrations_by_keyword(
                    keyword, start_date_str, end_date_str, max_results=50
                )
                
                all_businesses.extend(businesses)
                keyword_results[keyword] = len(businesses)
                
                print(f'{len(businesses)} found')
                
                # Rate limiting between searches (respect DFI server)
                if i < len(weekly_keywords):
                    import time
                    time.sleep(2)  # Reduced from 12 seconds for weekly batch
                    
            except Exception as e:
                print(f'ERROR: {e}')
                logging.error(f'Error collecting {keyword}: {e}')
                continue
        
        # Remove duplicates based on business_id
        unique_businesses = []
        seen_ids = set()
        
        for business in all_businesses:
            if business.business_id not in seen_ids:
                unique_businesses.append(business)
                seen_ids.add(business.business_id)
        
        duplicates_removed = len(all_businesses) - len(unique_businesses)
        
        print(f'\n📊 Collection Summary:')
        print(f'   Total found: {len(all_businesses)}')
        print(f'   Duplicates removed: {duplicates_removed}')
        print(f'   Unique businesses: {len(unique_businesses)}')
        
        # Show breakdown by keyword
        print(f'\n📋 Results by business type:')
        for keyword, count in keyword_results.items():
            if count > 0:
                print(f'   • {keyword}: {count}')
        
        if not unique_businesses:
            print('\nℹ️  No new businesses found this week')
            logging.info('Weekly collection completed - no new businesses found')
            return True
        
        # Check for existing businesses in BigQuery to avoid true duplicates
        table_id = "location-optimizer-1.raw_business_data.dfi_business_registrations"
        
        print(f'\n🔍 Checking for existing businesses in BigQuery...')
        
        existing_ids_query = f"""
        SELECT DISTINCT business_id 
        FROM `{table_id}` 
        WHERE source = 'DFI'
        """
        
        existing_results = client.query(existing_ids_query).result()
        existing_ids = {row.business_id for row in existing_results}
        
        # Filter out businesses that already exist
        new_businesses = [b for b in unique_businesses if b.business_id not in existing_ids]
        already_exist = len(unique_businesses) - len(new_businesses)
        
        print(f'   Existing in DB: {already_exist}')
        print(f'   New to add: {len(new_businesses)}')
        
        if not new_businesses:
            print('\n✅ All businesses already in database - no updates needed')
            logging.info('Weekly collection completed - all businesses already in database')
            return True
        
        # Prepare new businesses for BigQuery
        print(f'\n💾 Preparing {len(new_businesses)} new businesses for BigQuery...')
        
        rows_to_insert = []
        for business in new_businesses:
            # Parse registration date
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
        
        # Insert into BigQuery
        print(f'📤 Loading into BigQuery...')
        errors = client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            print(f'❌ BigQuery insertion errors:')
            for error in errors:
                print(f'   • {error}')
                logging.error(f'BigQuery error: {error}')
            return False
        
        print(f'✅ Successfully loaded {len(rows_to_insert)} new businesses!')
        
        # Show summary of new businesses
        business_types = {}
        for business in new_businesses:
            btype = business.business_type or 'Unclassified'
            business_types[btype] = business_types.get(btype, 0) + 1
        
        print(f'\n🎯 New Business Types Added:')
        for btype, count in sorted(business_types.items()):
            print(f'   • {btype}: {count}')
        
        # Show sample new businesses
        print(f'\n📝 Sample New Businesses:')
        for i, business in enumerate(new_businesses[:5], 1):
            print(f'   {i}. {business.business_name}')
            print(f'      Type: {business.business_type} | Date: {business.registration_date}')
            print(f'      Status: {business.status} | ID: {business.business_id}')
        
        if len(new_businesses) > 5:
            print(f'   ... and {len(new_businesses) - 5} more')
        
        # Log success
        logging.info(f'Weekly collection completed successfully - {len(new_businesses)} new businesses added')
        
        print(f'\n🎉 Weekly DFI collection completed successfully!')
        print(f'📊 Database updated with {len(new_businesses)} new Wisconsin businesses')
        
        return True
        
    except Exception as e:
        error_msg = f'Weekly DFI collection failed: {e}'
        print(f'❌ {error_msg}')
        logging.error(error_msg)
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Ensure logs directory exists
    os.makedirs('/workspaces/Test_for_Claude/Business/wisconsin_data_collection/logs', exist_ok=True)
    
    success = weekly_dfi_collection()
    if success:
        print('\n✅ Weekly collection completed successfully!')
    else:
        print('\n❌ Weekly collection failed - check logs')