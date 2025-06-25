#!/usr/bin/env python3
import os
from google.cloud import bigquery
from datetime import datetime
import pandas as pd

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json'

client = bigquery.Client(project="location-optimizer-1")

print("=" * 80)
print("BIGQUERY DATA INVENTORY")
print("=" * 80)

datasets = list(client.list_datasets())
print(f"\nFound {len(datasets)} dataset(s):")

for dataset in datasets:
    dataset_id = dataset.dataset_id
    print(f"\nDataset: {dataset_id}")
    print("-" * 40)
    
    tables = list(client.list_tables(dataset_id))
    print(f"Tables in {dataset_id}: {len(tables)}")
    
    for table in tables:
        table_ref = client.dataset(dataset_id).table(table.table_id)
        table_obj = client.get_table(table_ref)
        
        print(f"\n  Table: {table.table_id}")
        print(f"  - Total rows: {table_obj.num_rows:,}")
        print(f"  - Size: {table_obj.num_bytes / (1024*1024):.2f} MB")
        print(f"  - Created: {table_obj.created}")
        print(f"  - Last modified: {table_obj.modified}")
        
        # Get date range for tables with date columns
        date_columns = []
        for field in table_obj.schema:
            if field.field_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                date_columns.append(field.name)
        
        if date_columns and table_obj.num_rows > 0:
            print(f"  - Date columns found: {', '.join(date_columns)}")
            
            # Query first date column for range
            date_col = date_columns[0]
            query = f"""
            SELECT 
                MIN({date_col}) as earliest_date,
                MAX({date_col}) as latest_date
            FROM `{client.project}.{dataset_id}.{table.table_id}`
            WHERE {date_col} IS NOT NULL
            """
            
            try:
                result = client.query(query).result()
                for row in result:
                    if row.earliest_date and row.latest_date:
                        print(f"  - Date range ({date_col}): {row.earliest_date} to {row.latest_date}")
            except Exception as e:
                print(f"  - Could not query date range: {str(e)}")
        
        # Show sample of columns
        print(f"  - Columns ({len(table_obj.schema)}):")
        for i, field in enumerate(table_obj.schema[:5]):
            print(f"    • {field.name} ({field.field_type})")
        if len(table_obj.schema) > 5:
            print(f"    • ... and {len(table_obj.schema) - 5} more columns")

print("\n" + "=" * 80)