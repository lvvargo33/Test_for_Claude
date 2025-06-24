#!/usr/bin/env python3
"""
Show all BigQuery tables and their contents
"""

from google.cloud import bigquery

def show_bigquery_tables():
    print('📊 BigQuery Tables and Data Overview')
    print('=' * 50)
    
    try:
        client = bigquery.Client()
        project_id = "location-optimizer-1"
        
        # List all datasets
        datasets = list(client.list_datasets())
        
        print(f'🗂️  Datasets in project {project_id}:')
        
        for dataset in datasets:
            dataset_id = dataset.dataset_id
            print(f'\n📁 Dataset: {dataset_id}')
            
            # List tables in each dataset
            dataset_ref = client.dataset(dataset_id)
            tables = list(client.list_tables(dataset_ref))
            
            if not tables:
                print(f'   (No tables)')
                continue
            
            for table in tables:
                table_ref = dataset_ref.table(table.table_id)
                table_obj = client.get_table(table_ref)
                
                print(f'\n   📋 Table: {table.table_id}')
                print(f'      Rows: {table_obj.num_rows:,}')
                print(f'      Created: {table_obj.created}')
                print(f'      Size: {table_obj.num_bytes:,} bytes')
                
                # Show data sources in the table
                if table_obj.num_rows > 0:
                    try:
                        source_query = f"""
                        SELECT 
                            source,
                            COUNT(*) as count
                        FROM `{project_id}.{dataset_id}.{table.table_id}`
                        GROUP BY source
                        ORDER BY count DESC
                        """
                        
                        source_results = client.query(source_query).result()
                        
                        print(f'      Data sources:')
                        for row in source_results:
                            print(f'        • {row.source}: {row.count:,} records')
                            
                    except Exception as e:
                        print(f'        (Could not analyze data sources: {e})')
                
                # Show sample data for DFI table
                if table.table_id == 'dfi_business_registrations' and table_obj.num_rows > 0:
                    print(f'\n      🎯 Sample DFI Data:')
                    
                    sample_query = f"""
                    SELECT 
                        business_name,
                        business_type,
                        registration_date,
                        status
                    FROM `{project_id}.{dataset_id}.{table.table_id}`
                    WHERE source = 'DFI'
                    ORDER BY registration_date DESC
                    LIMIT 5
                    """
                    
                    sample_results = client.query(sample_query).result()
                    
                    for i, row in enumerate(sample_results, 1):
                        print(f'        {i}. {row.business_name}')
                        print(f'           Type: {row.business_type} | Date: {row.registration_date}')
        
        # Summary
        print(f'\n📈 Summary:')
        
        # Check specific tables with business data
        business_tables = [
            'raw_business_data.sba_loans',
            'raw_business_data.business_licenses', 
            'raw_business_data.dfi_business_registrations'
        ]
        
        for table_path in business_tables:
            try:
                table_id = f"{project_id}.{table_path}"
                table = client.get_table(table_id)
                
                if table.num_rows > 0:
                    # Get data sources
                    source_query = f"""
                    SELECT 
                        source,
                        COUNT(*) as count,
                        MIN(COALESCE(registration_date, approval_date, license_issue_date)) as earliest_date,
                        MAX(COALESCE(registration_date, approval_date, license_issue_date)) as latest_date
                    FROM `{table_id}`
                    GROUP BY source
                    ORDER BY count DESC
                    """
                    
                    results = client.query(source_query).result()
                    
                    print(f'\n✓ {table_path}:')
                    for row in results:
                        print(f'   • {row.source}: {row.count:,} records')
                        if row.earliest_date and row.latest_date:
                            print(f'     Date range: {row.earliest_date} to {row.latest_date}')
                        
            except Exception as e:
                print(f'\n❌ {table_path}: Not found or error')
        
        return True
        
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    show_bigquery_tables()