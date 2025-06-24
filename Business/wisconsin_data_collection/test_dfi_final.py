#!/usr/bin/env python3
"""
Final test of DFI collector - shows real Wisconsin business data without BigQuery dependency
"""

import os
import sys
sys.path.append('.')

from dfi_collector import DFIBusinessCollector
import logging
from datetime import datetime, timedelta

def test_dfi_final():
    print('🎉 Final DFI Data Collection Test')
    print('=' * 50)
    
    # Set up logging
    logging.basicConfig(level=logging.ERROR)  # Only show errors to reduce noise
    
    try:
        # Initialize DFI collector
        collector = DFIBusinessCollector()
        
        # Test single keyword search for restaurants in last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')
        
        print(f'🍕 Testing RESTAURANT search ({start_date_str} to {end_date_str})...')
        
        # Disable duplicate checking to avoid BigQuery dependency
        original_check_duplicates = collector._check_for_duplicates
        collector._check_for_duplicates = lambda name, date: False
        
        restaurant_businesses = collector.search_registrations_by_keyword('RESTAURANT', start_date_str, end_date_str)
        
        print(f'📊 Found {len(restaurant_businesses)} restaurant businesses')
        
        if restaurant_businesses:
            print(f'\n🎯 Recent Wisconsin Restaurant Registrations:')
            for i, business in enumerate(restaurant_businesses[:10]):
                print(f'   {i+1}. {business.business_name}')
                print(f'      • Entity: {business.entity_type}')
                print(f'      • Registered: {business.registration_date}')
                print(f'      • Status: {business.status}')
                print(f'      • ID: {business.business_id}')
                print(f'      • Business Type: {business.business_type}')
                print()
            
            if len(restaurant_businesses) > 10:
                print(f'   ... and {len(restaurant_businesses) - 10} more restaurants')
        
        # Test one more keyword - SALON
        print(f'\n💇 Testing SALON search...')
        salon_businesses = collector.search_registrations_by_keyword('SALON', start_date_str, end_date_str)
        
        print(f'📊 Found {len(salon_businesses)} salon businesses')
        
        if salon_businesses:
            print(f'\n✨ Recent Wisconsin Salon Registrations:')
            for i, business in enumerate(salon_businesses[:5]):
                print(f'   {i+1}. {business.business_name}')
                print(f'      • Registered: {business.registration_date} | Status: {business.status}')
                print(f'      • Business Type: {business.business_type}')
                print()
        
        total_found = len(restaurant_businesses) + len(salon_businesses)
        
        print(f'\n📈 Summary:')
        print(f'   • {len(restaurant_businesses)} restaurant businesses')
        print(f'   • {len(salon_businesses)} salon businesses')
        print(f'   • {total_found} total target businesses in last 30 days')
        print(f'   • Average: {total_found / 30:.1f} target businesses per day')
        
        if total_found > 0:
            print(f'\n✅ DFI data collection working successfully!')
            print(f'🔄 Ready to integrate with Wisconsin data collection pipeline')
            return True
            
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
        
    return False

if __name__ == "__main__":
    success = test_dfi_final()
    if success:
        print('\n🎉 Wisconsin DFI business data collection is working!')
        print('📊 Real recent business registrations successfully retrieved')
    else:
        print('\n🔧 DFI collection needs troubleshooting')