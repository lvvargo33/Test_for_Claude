#!/usr/bin/env python3
"""
Test real SBA data collection
"""

import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
import sys
sys.path.append('.')
from models import SBALoanRecord

def test_real_sba_data():
    print('🔄 Testing Real SBA Data Download')
    print('=' * 50)
    
    # Use the working 504 URL
    url = "https://data.sba.gov/dataset/0ff8e8e9-b967-4f4e-987c-6ac78c575087/resource/4ad7f0f1-9da6-4d90-8bdb-89a6f821a1a9/download/foia-504-fy2010-present-asof-250331.csv"
    
    try:
        print(f"📥 Downloading from: {url}")
        response = requests.get(url, timeout=60)
        print(f"✅ Downloaded {len(response.content)} bytes")
        
        # Parse CSV
        csv_reader = csv.DictReader(StringIO(response.text))
        
        wisconsin_loans = []
        total_count = 0
        wi_count = 0
        
        # Look for Wisconsin loans
        for row in csv_reader:
            total_count += 1
            
            if row.get('BorrState', '').strip().upper() == 'WI':
                wi_count += 1
                
                # Parse loan record
                try:
                    loan_data = {
                        'loan_id': row.get('LocationID', f'504-{wi_count}'),
                        'borrower_name': row.get('BorrName', '').strip(),
                        'borrower_city': row.get('BorrCity', '').strip(),
                        'borrower_state': 'WI',
                        'borrower_zip': row.get('BorrZip', '').strip(),
                        'loan_amount': float(row.get('GrossApproval', 0) or 0),
                        'approval_date': datetime.strptime(row.get('ApprovalDate'), '%m/%d/%Y').date(),
                        'program_type': '504',
                        'naics_code': row.get('NaicsCode', ''),
                        'business_type': row.get('BusinessType', ''),
                        'jobs_supported': int(row.get('JobsSupported', 0) or 0),
                        'franchise_name': row.get('FranchiseName', '').strip(),
                        'lender_name': row.get('ThirdPartyLender_Name', '').strip()
                    }
                    
                    if loan_data['borrower_name'] and loan_data['loan_amount'] > 0:
                        loan_record = SBALoanRecord(**loan_data)
                        wisconsin_loans.append(loan_record)
                        
                        # Show first few records
                        if len(wisconsin_loans) <= 5:
                            print(f"   📋 {loan_record.borrower_name} in {loan_record.borrower_city}")
                            print(f"      💰 ${loan_record.loan_amount:,.0f} on {loan_record.approval_date}")
                            print(f"      🏭 {loan_record.business_type} ({loan_record.naics_code})")
                            if loan_record.franchise_name:
                                print(f"      🔗 Franchise: {loan_record.franchise_name}")
                            print()
                
                except Exception as e:
                    print(f"   ⚠️  Error parsing loan record: {e}")
                    continue
            
            # Limit processing for testing
            if total_count > 50000:  # Process first 50k records
                break
        
        print(f"📊 Results:")
        print(f"   Total records processed: {total_count:,}")
        print(f"   Wisconsin loans found: {wi_count}")
        print(f"   Valid loan records: {len(wisconsin_loans)}")
        
        if wisconsin_loans:
            print(f"\n🎯 Top Wisconsin SBA 504 Loans:")
            for i, loan in enumerate(sorted(wisconsin_loans, key=lambda x: x.loan_amount, reverse=True)[:10]):
                print(f"   {i+1}. {loan.borrower_name} - ${loan.loan_amount:,.0f}")
        
        return len(wisconsin_loans)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 0

if __name__ == "__main__":
    result = test_real_sba_data()
    print(f"\n✅ Successfully processed {result} Wisconsin SBA loans!")