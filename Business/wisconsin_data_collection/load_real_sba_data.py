#!/usr/bin/env python3
"""
Load real SBA data to BigQuery
"""

import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
import sys
import os
sys.path.append('.')
from models import SBALoanRecord
from google.cloud import bigquery
import pandas as pd

def load_real_sba_data():
    print('🔄 Loading Real SBA Data to BigQuery')
    print('=' * 50)
    
    # Initialize BigQuery client
    client = bigquery.Client(project="location-optimizer-1")
    
    # URL for SBA 504 data
    url = "https://data.sba.gov/dataset/0ff8e8e9-b967-4f4e-987c-6ac78c575087/resource/4ad7f0f1-9da6-4d90-8bdb-89a6f821a1a9/download/foia-504-fy2010-present-asof-250331.csv"
    
    try:
        print(f"📥 Downloading SBA 504 data...")
        response = requests.get(url, timeout=120)
        print(f"✅ Downloaded {len(response.content)} bytes")
        
        # Parse CSV and collect Wisconsin loans
        csv_reader = csv.DictReader(StringIO(response.text))
        
        sba_records = []
        total_count = 0
        wi_count = 0
        
        for row in csv_reader:
            total_count += 1
            
            if row.get('BorrState', '').strip().upper() == 'WI':
                wi_count += 1
                
                try:
                    # Parse loan record
                    loan_data = {
                        'loan_id': row.get('LocationID', f'504-{wi_count}'),
                        'borrower_name': row.get('BorrName', '').strip(),
                        'borrower_city': row.get('BorrCity', '').strip(),
                        'borrower_state': 'WI',
                        'borrower_zip': row.get('BorrZip', '').strip(),
                        'loan_amount': float(row.get('GrossApproval', 0) or 0),
                        'approval_date': row.get('ApprovalDate', ''),
                        'program_type': '504',
                        'naics_code': row.get('NaicsCode', ''),
                        'business_type': row.get('BusinessType', ''),
                        'jobs_supported': int(row.get('JobsSupported', 0) or 0),
                        'franchise_name': row.get('FranchiseName', '').strip(),
                        'lender_name': row.get('ThirdPartyLender_Name', '').strip(),
                        'borrower_address': row.get('BorrStreet', '').strip(),
                        'data_source': 'SBA_504_FOIA'
                    }
                    
                    # Parse approval date
                    try:
                        loan_data['approval_date'] = datetime.strptime(loan_data['approval_date'], '%m/%d/%Y').date()
                    except:
                        continue  # Skip records with bad dates
                    
                    if loan_data['borrower_name'] and loan_data['loan_amount'] > 0:
                        sba_records.append(loan_data)
                        
                except Exception as e:
                    continue  # Skip problematic records
            
            # Limit for testing
            if total_count > 100000:  # Process up to 100k records
                break
        
        print(f"📊 Processed {total_count:,} total records")
        print(f"🎯 Found {len(sba_records)} valid Wisconsin SBA loans")
        
        if sba_records:
            # Convert to DataFrame for BigQuery
            df = pd.DataFrame(sba_records)
            
            # Add metadata
            df['data_extraction_date'] = datetime.now()
            
            print(f"💾 Loading {len(df)} records to BigQuery...")
            
            # Load to BigQuery
            table_id = "location-optimizer-1.raw_business_data.sba_loan_approvals"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )
            
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for job to complete
            
            print(f"✅ Successfully loaded {len(df)} SBA loan records to BigQuery!")
            
            # Show sample records
            print(f"\n📋 Sample Wisconsin SBA Loans:")
            for i, record in enumerate(sba_records[:5]):
                print(f"   {i+1}. {record['borrower_name']} in {record['borrower_city']}")
                print(f"      💰 ${record['loan_amount']:,.0f} approved {record['approval_date']}")
            
            return len(sba_records)
        else:
            print("❌ No valid SBA records found")
            return 0
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 0

if __name__ == "__main__":
    # Set up environment
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './location-optimizer-1-449414f93a5a.json'
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'location-optimizer-1'
    
    result = load_real_sba_data()
    print(f"\n🎉 Loaded {result} real Wisconsin SBA loans to BigQuery!")