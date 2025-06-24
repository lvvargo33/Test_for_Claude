#!/usr/bin/env python3
"""
Check the current schema and data in the DFI table
"""

from google.cloud import bigquery

def check_table_schema():
    print('🔍 Checking DFI Table Schema and Data')
    print('=' * 50)
    
    try:
        client = bigquery.Client()
        table_id = "location-optimizer-1.raw_business_data.dfi_business_registrations"
        
        # Check if table exists and get schema
        try:
            table = client.get_table(table_id)
            print(f'✓ Table exists: {table_id}')
            print(f'  Rows: {table.num_rows}')
            print(f'  Created: {table.created}')
            print(f'  Modified: {table.modified}')
            
            print(f'\n📋 Table Schema:')
            for field in table.schema:
                print(f'  • {field.name}: {field.field_type}')
                
        except Exception as e:
            print(f'❌ Table not found or error: {e}')
            return False
        
        # Check current data
        simple_query = f"""
        SELECT 
            source,
            COUNT(*) as count
        FROM `{table_id}`
        GROUP BY source
        ORDER BY count DESC
        """
        
        results = client.query(simple_query).result()
        
        print(f'\n📊 Current Data by Source:')
        total_rows = 0
        for row in results:
            print(f'  • {row.source}: {row.count} records')
            total_rows += row.count
        
        print(f'\n📈 Total records: {total_rows}')
        
        # Check for any DFI records
        dfi_query = f"""
        SELECT 
            business_name,
            business_type,
            registration_date,
            status
        FROM `{table_id}`
        WHERE source = 'DFI'
        ORDER BY business_name
        LIMIT 10
        """
        
        dfi_results = client.query(dfi_query).result()
        
        dfi_count = 0
        print(f'\n🎯 DFI Records:')
        for row in dfi_results:
            dfi_count += 1
            print(f'  {dfi_count}. {row.business_name}')
            print(f'     Type: {row.business_type} | Date: {row.registration_date}')
            print(f'     Status: {row.status}')
            print()
        
        if dfi_count == 0:
            print('  No DFI records found yet')
            return False
        else:
            print(f'✅ Found {dfi_count} DFI records (showing first 10)')
            return True
            
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    check_table_schema()