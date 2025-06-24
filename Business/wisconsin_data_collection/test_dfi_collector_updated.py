#!/usr/bin/env python3
"""
Test the updated DFI collector with real keyword searches
"""

import os
import sys
sys.path.append('.')

from dfi_collector import DFIBusinessCollector
import logging

def test_updated_dfi_collector():
    print('🔄 Testing Updated DFI Collector')
    print('=' * 50)
    
    # Set up logging to see detailed information
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    
    try:
        # Initialize DFI collector
        collector = DFIBusinessCollector()
        
        print('🧪 Testing single keyword search first...')
        
        # Test single keyword search (RESTAURANT) for last 30 days
        from datetime import datetime, timedelta
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')
        
        print(f'🔍 Testing RESTAURANT search for {start_date_str} to {end_date_str}...')
        
        restaurant_businesses = collector.search_registrations_by_keyword('RESTAURANT', start_date_str, end_date_str)
        
        print(f'📊 Found {len(restaurant_businesses)} restaurant businesses')
        
        if restaurant_businesses:
            print(f'\n🎯 Sample Restaurant Businesses:')
            for i, business in enumerate(restaurant_businesses[:5]):
                print(f'   {i+1}. {business.business_name}')
                print(f'      ID: {business.business_id} | Type: {business.entity_type}')
                print(f'      Date: {business.registration_date} | Status: {business.status}')
                print(f'      Business Type: {business.business_type}')
                print()
        
        if len(restaurant_businesses) > 0:
            print(f'✅ Single keyword search working!')
            
            print(f'\n🔄 Now testing full collection (last 30 days with limited keywords)...')
            
            # Temporarily reduce keywords for testing
            original_keywords = collector.collect_recent_registrations.__code__.co_consts
            
            # Test with just a few keywords
            all_businesses = collector.collect_recent_registrations(days_back=30)
            
            print(f'\n📊 Full Collection Results:')
            print(f'   Total target businesses found: {len(all_businesses)}')
            
            if all_businesses:
                # Group by business type
                type_counts = {}
                for business in all_businesses:
                    btype = business.business_type or 'Unclassified'
                    type_counts[btype] = type_counts.get(btype, 0) + 1
                
                print(f'\n📋 Business Types Found:')
                for btype, count in sorted(type_counts.items()):
                    print(f'   • {btype}: {count} businesses')
                
                print(f'\n🎯 Sample Recent Target Businesses:')
                for i, business in enumerate(all_businesses[:10]):
                    print(f'   {i+1}. {business.business_name}')
                    print(f'      Type: {business.business_type} | Date: {business.registration_date}')
                    print(f'      Status: {business.status} | ID: {business.business_id}')
                    print()
                
                print(f'✅ DFI data collection is working with real data!')
                return True
            else:
                print(f'ℹ️  No target businesses found in recent registrations')
        else:
            print(f'❌ Single keyword search returned no results')
            
    except Exception as e:
        print(f'❌ Error testing DFI collector: {e}')
        import traceback
        traceback.print_exc()
        
    return False

if __name__ == "__main__":
    success = test_updated_dfi_collector()
    if success:
        print('\n🎉 Updated DFI collector working successfully with real data!')
    else:
        print('\n🔧 DFI collector needs further refinement')