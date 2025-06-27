#!/usr/bin/env python3
"""Test BigQuery credentials"""

from google.cloud import bigquery
from google.oauth2 import service_account
import json

# Load credentials
credentials_path = "location-optimizer-1-449414f93a5a.json"
with open(credentials_path, 'r') as f:
    creds_info = json.load(f)

print(f"Project ID: {creds_info['project_id']}")
print(f"Service Account: {creds_info['client_email']}")

# Create credentials object
credentials = service_account.Credentials.from_service_account_file(
    credentials_path,
    scopes=["https://www.googleapis.com/auth/bigquery"]
)

# Create BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=creds_info['project_id']
)

# Test connection by listing datasets
print("\nTesting BigQuery connection...")
try:
    datasets = list(client.list_datasets())
    print(f"✓ Successfully connected to BigQuery!")
    print(f"✓ Found {len(datasets)} datasets in project '{creds_info['project_id']}':")
    
    for dataset in datasets:
        print(f"  - {dataset.dataset_id}")
        
        # List tables in each dataset (limit to first 5)
        tables = list(client.list_tables(dataset.reference))[:5]
        if tables:
            print(f"    Tables: {', '.join([t.table_id for t in tables])}")
            if len(list(client.list_tables(dataset.reference))) > 5:
                print(f"    ... and {len(list(client.list_tables(dataset.reference))) - 5} more tables")
                
except Exception as e:
    print(f"✗ Failed to connect to BigQuery: {str(e)}")
    print(f"  Error type: {type(e).__name__}")