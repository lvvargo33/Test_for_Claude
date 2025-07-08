#!/usr/bin/env python3
"""Test BigQuery credentials setup"""

import os
import json
from google.cloud import bigquery

# Check environment variable
creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
print(f"GOOGLE_APPLICATION_CREDENTIALS: {creds_path}")

if creds_path and os.path.exists(creds_path):
    print("✓ Credentials file exists")
    
    # Load and check credentials
    with open(creds_path, 'r') as f:
        creds_info = json.load(f)
    
    print(f"✓ Project ID: {creds_info['project_id']}")
    print(f"✓ Service Account: {creds_info['client_email']}")
    
    # Test BigQuery connection
    try:
        client = bigquery.Client()
        datasets = list(client.list_datasets())
        print(f"✓ Successfully connected to BigQuery!")
        print(f"✓ Found {len(datasets)} datasets")
        
        for dataset in datasets[:3]:  # Show first 3
            print(f"  - {dataset.dataset_id}")
            
    except Exception as e:
        print(f"✗ BigQuery connection failed: {e}")
else:
    print("✗ Credentials file not found or environment variable not set")