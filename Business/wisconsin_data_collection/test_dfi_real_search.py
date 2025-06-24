#!/usr/bin/env python3
"""
Test real DFI search with date range to get actual business data
"""

import os
import sys
sys.path.append('.')

from dfi_collector import DFIBusinessCollector
import logging

def test_real_dfi_search():
    print('🔍 Testing Real DFI Business Search')
    print('=' * 50)
    
    # Set up logging to see detailed information
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize DFI collector
        collector = DFIBusinessCollector()
        
        print('📅 Searching for recent business registrations...')
        print('   Looking back 90 days for new businesses')
        
        # Try to collect real DFI data
        registrations = collector.collect_recent_registrations(days_back=90)
        
        print(f'\n📊 Collection Results:')
        print(f'   Total registrations found: {len(registrations)}')
        
        if registrations:
            print(f'\n🎯 Target Business Registrations:')
            for i, reg in enumerate(registrations[:10]):
                print(f'   {i+1}. {reg.business_name}')
                print(f'      Type: {reg.entity_type} | Business Type: {reg.business_type}')
                print(f'      Date: {reg.registration_date} | Status: {reg.status}')
                print(f'      ID: {reg.business_id}')
                print()
        
        # Test with a smaller date range to see if we get results
        print(f'\n🔍 Trying smaller date range (last 30 days)...')
        recent_registrations = collector.collect_recent_registrations(days_back=30)
        print(f'   Found: {len(recent_registrations)} registrations')
        
        # Test with a larger date range 
        print(f'\n🔍 Trying larger date range (last 180 days)...')
        older_registrations = collector.collect_recent_registrations(days_back=180)
        print(f'   Found: {len(older_registrations)} registrations')
        
        if not registrations and not recent_registrations and not older_registrations:
            print(f'\n⚠️  No registrations found in any date range.')
            print(f'   This could indicate:')
            print(f'   • DFI search form may need additional refinement')
            print(f'   • Result parsing needs adjustment')
            print(f'   • DFI may require specific search criteria')
            print(f'   • Rate limiting or access restrictions')
            
            print(f'\n🔧 Debugging suggestions:')
            print(f'   • Check the saved HTML response file')
            print(f'   • Verify form field names are correct')
            print(f'   • Test with manual search on DFI website')
            
        else:
            print(f'\n✅ Successfully collected real DFI data!')
            
            # Save successful results if any
            if registrations:
                print(f'\n💾 Would save {len(registrations)} registrations to BigQuery')
                
        return len(registrations) > 0
        
    except Exception as e:
        print(f'❌ Error testing DFI search: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_real_dfi_search()
    if success:
        print('\n🎉 Real DFI data collection successful!')
    else:
        print('\n🔧 DFI data collection needs refinement')