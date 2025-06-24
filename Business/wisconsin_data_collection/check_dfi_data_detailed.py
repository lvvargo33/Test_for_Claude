#!/usr/bin/env python3
"""
Detailed check of DFI data in BigQuery
"""

from google.cloud import bigquery

def check_dfi_data_detailed():
    print('🔍 Detailed DFI Data Check')
    print('=' * 40)
    
    try:
        client = bigquery.Client()
        
        # Check the exact table
        table_id = "location-optimizer-1.raw_business_data.dfi_business_registrations"
        
        # Get table info
        table = client.get_table(table_id)
        print(f'📋 Table: {table_id}')
        print(f'   Rows: {table.num_rows}')
        print(f'   Created: {table.created}')
        print(f'   Modified: {table.modified}')
        print(f'   Size: {table.num_bytes} bytes')
        
        # Check all data in the table
        all_data_query = f"""
        SELECT 
            *
        FROM `{table_id}`
        LIMIT 10
        """
        
        print(f'\n📊 All data in table:')
        results = client.query(all_data_query).result()
        
        row_count = 0
        for row in results:
            row_count += 1
            print(f'   {row_count}. {row.business_name}')
            print(f'      ID: {row.business_id} | Type: {row.business_type}')
            print(f'      Date: {row.registration_date} | Status: {row.status}')
            print(f'      Source: {row.source}')
            print()
        
        if row_count == 0:
            print('   No data found in table')
            
            # Check table schema
            print(f'\n📋 Table Schema:')
            for field in table.schema:
                print(f'   • {field.name}: {field.field_type} ({field.mode})')
        else:
            print(f'   Total rows shown: {row_count}')
        
        # Check for any recent insertions (in case data was loaded but not visible yet)
        recent_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN source = 'DFI' THEN 1 END) as dfi_rows,
            MAX(data_extraction_date) as last_extraction
        FROM `{table_id}`
        """
        
        recent_results = client.query(recent_query).result()
        
        for row in recent_results:
            print(f'\n📈 Table Summary:')
            print(f'   Total rows: {row.total_rows}')
            print(f'   DFI rows: {row.dfi_rows}')
            print(f'   Last extraction: {row.last_extraction}')
        
        return row_count > 0
        
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = check_dfi_data_detailed()
    if success:
        print('\n✅ DFI data found in BigQuery')
    else:
        print('\n❌ No DFI data found')