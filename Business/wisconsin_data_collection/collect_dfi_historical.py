#!/usr/bin/env python3
"""
Collect DFI Historical Data
===========================

Systematically collect historical Wisconsin business registrations by 
modifying date ranges in our existing DFI collection system.
"""

import os
import sys
sys.path.append('.')

from dfi_collector import DFIBusinessCollector
from google.cloud import bigquery
import logging
from datetime import datetime, timedelta
import time

def collect_dfi_historical_data(years_back=5, batch_size_months=3):
    """
    Collect historical DFI data by systematically searching date ranges
    
    Args:
        years_back (int): How many years back to collect (default 5)
        batch_size_months (int): How many months to collect in each batch (default 3)
    """
    
    print(f'📚 DFI HISTORICAL DATA COLLECTION')
    print('=' * 60)
    print(f'🎯 Target: {years_back} years of historical data')
    print(f'📅 Batch size: {batch_size_months} months per collection')
    print(f'🕒 Start time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/workspaces/Test_for_Claude/Business/wisconsin_data_collection/logs/dfi_historical_collection.log'),
            logging.StreamHandler()
        ]
    )
    
    try:
        # Initialize clients
        print('🔧 Initializing BigQuery and DFI collector...')
        client = bigquery.Client()
        collector = DFIBusinessCollector()
        
        # Calculate date ranges
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years_back * 365)
        
        print(f'📅 Collection period: {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
        
        # Priority keywords for historical collection
        historical_keywords = [
            'RESTAURANT', 'PIZZA', 'CAFE', 'BAR', 'FOOD', 'BREWERY',
            'SALON', 'SPA', 'BEAUTY', 'HAIR', 'NAIL',
            'FITNESS', 'GYM', 'YOGA', 'WELLNESS',
            'AUTO', 'REPAIR', 'GARAGE',
            'RETAIL', 'STORE', 'SHOP', 'BOUTIQUE',
            'HOTEL', 'MOTEL', 'INN',
            'CLEANING', 'LAUNDRY',
            'MEDICAL', 'DENTAL', 'CLINIC',
            'CONSULTING', 'SERVICES',
            'CONSTRUCTION', 'CONTRACTOR'
        ]
        
        print(f'🔍 Searching {len(historical_keywords)} business types...')
        
        # Create date batches
        current_date = start_date
        date_batches = []
        
        while current_date < end_date:
            batch_end = min(current_date + timedelta(days=batch_size_months * 30), end_date)
            date_batches.append((current_date, batch_end))
            current_date = batch_end + timedelta(days=1)
        
        print(f'📊 Created {len(date_batches)} date batches for collection')
        
        all_historical_businesses = []
        batch_summary = {}
        
        # Disable duplicate checking during collection (we'll handle it in BigQuery)
        collector._check_for_duplicates = lambda name, date: False
        
        # Collect data for each date batch
        for batch_num, (batch_start, batch_end) in enumerate(date_batches, 1):
            batch_start_str = batch_start.strftime('%m/%d/%Y')
            batch_end_str = batch_end.strftime('%m/%d/%Y')
            
            print(f'\\n📦 BATCH {batch_num}/{len(date_batches)}: {batch_start_str} - {batch_end_str}')
            print('-' * 50)
            
            batch_businesses = []
            batch_keyword_results = {}
            
            # Collect data for each keyword in this date range
            for keyword_num, keyword in enumerate(historical_keywords, 1):
                try:
                    print(f'   {keyword_num:2d}/{len(historical_keywords)} {keyword}...', end=' ')
                    
                    businesses = collector.search_registrations_by_keyword(
                        keyword, batch_start_str, batch_end_str, max_results=100
                    )
                    
                    batch_businesses.extend(businesses)
                    batch_keyword_results[keyword] = len(businesses)
                    
                    print(f'{len(businesses)} found')
                    
                    # Rate limiting between searches (respect DFI server)
                    if keyword_num < len(historical_keywords):
                        time.sleep(3)  # Slightly longer delay for historical collection
                        
                except Exception as e:
                    print(f'ERROR: {e}')
                    logging.error(f'Error collecting {keyword} for batch {batch_num}: {e}')
                    continue
            
            # Remove duplicates within this batch
            unique_batch_businesses = []
            seen_ids = set()
            
            for business in batch_businesses:
                if business.business_id not in seen_ids:
                    unique_batch_businesses.append(business)
                    seen_ids.add(business.business_id)
            
            batch_duplicates_removed = len(batch_businesses) - len(unique_batch_businesses)
            
            print(f'\\n   📊 Batch {batch_num} Summary:')
            print(f'      Total found: {len(batch_businesses)}')
            print(f'      Duplicates removed: {batch_duplicates_removed}')
            print(f'      Unique businesses: {len(unique_batch_businesses)}')
            
            # Show top keywords for this batch
            top_keywords = sorted(batch_keyword_results.items(), key=lambda x: x[1], reverse=True)[:5]
            if any(count > 0 for _, count in top_keywords):
                print(f'      Top types: {", ".join([f"{kw}({cnt})" for kw, cnt in top_keywords if cnt > 0])}')
            
            all_historical_businesses.extend(unique_batch_businesses)
            batch_summary[batch_num] = {
                'date_range': f'{batch_start_str} - {batch_end_str}',
                'businesses_found': len(unique_batch_businesses),
                'duplicates_removed': batch_duplicates_removed
            }
            
            # Brief pause between batches
            if batch_num < len(date_batches):
                print(f'   ⏸️  Pausing 10 seconds before next batch...')
                time.sleep(10)
        
        # Remove duplicates across all batches
        print(f'\\n🔧 Removing duplicates across all batches...')
        unique_all_businesses = []
        seen_all_ids = set()
        
        for business in all_historical_businesses:
            if business.business_id not in seen_all_ids:
                unique_all_businesses.append(business)
                seen_all_ids.add(business.business_id)
        
        total_duplicates_removed = len(all_historical_businesses) - len(unique_all_businesses)
        
        print(f'📊 OVERALL COLLECTION SUMMARY:')
        print(f'   Total businesses found: {len(all_historical_businesses):,}')
        print(f'   Cross-batch duplicates removed: {total_duplicates_removed:,}')
        print(f'   Unique historical businesses: {len(unique_all_businesses):,}')
        
        if not unique_all_businesses:
            print('\\nℹ️  No historical businesses found')
            logging.info('Historical collection completed - no businesses found')
            return True
        
        # Check for existing businesses in BigQuery
        table_id = "location-optimizer-1.raw_business_data.dfi_business_registrations"
        
        print(f'\\n🔍 Checking for existing businesses in BigQuery...')
        
        existing_ids_query = f"""
        SELECT DISTINCT business_id 
        FROM `{table_id}` 
        WHERE source = 'DFI'
        """
        
        try:
            existing_results = client.query(existing_ids_query).result()
            existing_ids = {row.business_id for row in existing_results}
            print(f'   Found {len(existing_ids):,} existing businesses in database')
        except Exception as e:
            print(f'   ⚠️  Could not check existing businesses: {e}')
            existing_ids = set()
        
        # Filter out businesses that already exist
        new_businesses = [b for b in unique_all_businesses if b.business_id not in existing_ids]
        already_exist = len(unique_all_businesses) - len(new_businesses)
        
        print(f'   Already in database: {already_exist:,}')
        print(f'   New to add: {len(new_businesses):,}')
        
        if not new_businesses:
            print('\\n✅ All historical businesses already in database - no updates needed')
            logging.info('Historical collection completed - all businesses already in database')
            return True
        
        # Prepare new businesses for BigQuery
        print(f'\\n💾 Preparing {len(new_businesses):,} new businesses for BigQuery...')
        
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
                'is_target_business': True,
                'collection_type': 'historical'
            }
            rows_to_insert.append(row)
        
        # Insert into BigQuery in batches (BigQuery has limits)
        batch_size = 1000
        successful_inserts = 0
        
        print(f'📤 Loading into BigQuery in batches of {batch_size:,}...')
        
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size
            
            print(f'   Loading batch {batch_num}/{total_batches} ({len(batch):,} rows)...', end=' ')
            
            try:
                errors = client.insert_rows_json(table_id, batch)
                
                if errors:
                    print(f'❌ ERRORS')
                    for error in errors[:3]:  # Show first 3 errors
                        print(f'      • {error}')
                        logging.error(f'BigQuery error in batch {batch_num}: {error}')
                    if len(errors) > 3:
                        print(f'      ... and {len(errors) - 3} more errors')
                else:
                    print(f'✅ SUCCESS')
                    successful_inserts += len(batch)
                    
            except Exception as e:
                print(f'❌ FAILED: {e}')
                logging.error(f'BigQuery batch {batch_num} failed: {e}')
            
            # Brief pause between BigQuery batches
            if i + batch_size < len(rows_to_insert):
                time.sleep(2)
        
        print(f'\\n✅ Successfully loaded {successful_inserts:,}/{len(rows_to_insert):,} historical businesses!')
        
        # Show summary statistics
        if new_businesses:
            # Business types breakdown
            business_types = {}
            for business in new_businesses:
                btype = business.business_type or 'Unclassified'
                business_types[btype] = business_types.get(btype, 0) + 1
            
            print(f'\\n🎯 Historical Business Types Added:')
            for btype, count in sorted(business_types.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f'   • {btype}: {count:,}')
            
            # Year breakdown
            year_counts = {}
            for business in new_businesses:
                try:
                    year = datetime.strptime(business.registration_date, '%m/%d/%Y').year
                    year_counts[year] = year_counts.get(year, 0) + 1
                except:
                    year_counts['Unknown'] = year_counts.get('Unknown', 0) + 1
            
            print(f'\\n📅 Historical Data by Year:')
            for year in sorted(year_counts.keys(), reverse=True):
                if year != 'Unknown':
                    print(f'   • {year}: {year_counts[year]:,} businesses')
            if 'Unknown' in year_counts:
                print(f'   • Unknown: {year_counts["Unknown"]:,} businesses')
        
        # Show batch summary
        print(f'\\n📦 Collection Batch Summary:')
        for batch_num, summary in batch_summary.items():
            print(f'   Batch {batch_num}: {summary["date_range"]} → {summary["businesses_found"]:,} businesses')
        
        # Log success
        logging.info(f'Historical collection completed successfully - {successful_inserts:,} new businesses added')
        
        print(f'\\n🎉 DFI historical collection completed successfully!')
        print(f'📊 Database updated with {successful_inserts:,} historical Wisconsin businesses')
        print(f'⏱️  Total runtime: {datetime.now().strftime("%H:%M:%S")}')
        
        return True
        
    except Exception as e:
        error_msg = f'Historical DFI collection failed: {e}'
        print(f'❌ {error_msg}')
        logging.error(error_msg)
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function - automatically collect 5 years of historical data"""
    
    print("🚀 DFI HISTORICAL DATA COLLECTION - 5 YEARS")
    print("=" * 60)
    print("Automatically collecting 5 years of Wisconsin business registration data")
    print()
    
    # Set credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548.json'
    
    # Ensure logs directory exists
    os.makedirs('/workspaces/Test_for_Claude/Business/wisconsin_data_collection/logs', exist_ok=True)
    
    print(f"🎯 Collecting 5 years of historical DFI data...")
    print(f"📅 Target period: {(datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")
    print(f"⏱️  Estimated time: 30-45 minutes")
    print(f"🔄 Rate limiting: 3 seconds between searches (server-friendly)")
    print()
    
    success = collect_dfi_historical_data(years_back=5, batch_size_months=3)
    
    if success:
        print('\n🎉 5-year historical collection completed successfully!')
        print('📊 Historical trend analysis now available')
        print('✅ Ready for client presentations and market intelligence')
    else:
        print('\n❌ Historical collection failed - check logs')

if __name__ == "__main__":
    main()