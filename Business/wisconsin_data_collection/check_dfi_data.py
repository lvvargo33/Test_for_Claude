#!/usr/bin/env python3
"""
Check if DFI data has been loaded into BigQuery
"""

from google.cloud import bigquery

def check_dfi_data():
    print('🔍 Checking DFI Data in BigQuery')
    print('=' * 40)
    
    try:
        client = bigquery.Client()
        
        # Check for DFI data
        query = """
        SELECT 
            source,
            business_type,
            COUNT(*) as count,
            MIN(registration_date) as earliest_date,
            MAX(registration_date) as latest_date,
            MAX(created_at) as last_loaded
        FROM `location-optimizer-1.raw_business_data.dfi_business_registrations`
        WHERE source = 'DFI'
        GROUP BY source, business_type
        ORDER BY count DESC
        """
        
        results = client.query(query).result()
        
        total_dfi = 0
        found_data = False
        
        for row in results:
            found_data = True
            print(f'✓ {row.business_type}: {row.count} businesses')
            print(f'  Date range: {row.earliest_date} to {row.latest_date}')
            print(f'  Last loaded: {row.last_loaded}')
            print()
            total_dfi += row.count
        
        if found_data:
            print(f'📊 Total DFI businesses in BigQuery: {total_dfi}')
            
            # Show recent examples
            sample_query = """
            SELECT 
                business_name,
                business_type,
                registration_date,
                status,
                business_id
            FROM `location-optimizer-1.raw_business_data.dfi_business_registrations`
            WHERE source = 'DFI'
            ORDER BY created_at DESC
            LIMIT 5
            """
            
            sample_results = client.query(sample_query).result()
            
            print(f'\n🎯 Recent DFI Business Examples:')
            for i, row in enumerate(sample_results, 1):
                print(f'  {i}. {row.business_name}')
                print(f'     Type: {row.business_type} | Date: {row.registration_date}')
                print(f'     Status: {row.status} | ID: {row.business_id}')
                print()
                
            return True
        else:
            print('ℹ️  No DFI data found in BigQuery yet')
            return False
            
    except Exception as e:
        print(f'❌ Error checking data: {e}')
        return False

if __name__ == "__main__":
    check_dfi_data()