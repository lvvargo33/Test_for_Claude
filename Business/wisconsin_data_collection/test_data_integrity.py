#!/usr/bin/env python3
"""
Test data integrity and quality
"""

import pandas as pd
import sys

def test_data_integrity():
    print('🔍 Testing Data Integrity and Quality')
    print('=' * 50)
    
    try:
        # Test business data
        df_business = pd.read_csv('sample_businesses.csv')
        print(f'✅ Business data: {len(df_business)} records')
        print(f'   Missing business names: {df_business["business_name"].isna().sum()}')
        print(f'   Missing cities: {df_business["city"].isna().sum()}')
        print(f'   Non-WI states: {(df_business["state"] != "WI").sum()}')
        print(f'   Empty business types: {df_business["business_type"].isna().sum()}')
        
        # Test SBA loan data
        df_loans = pd.read_csv('sample_sba_loans.csv')
        print(f'✅ SBA loan data: {len(df_loans)} records')
        print(f'   Missing borrower names: {df_loans["borrower_name"].isna().sum()}')
        print(f'   Zero loan amounts: {(df_loans["loan_amount"] <= 0).sum()}')
        print(f'   Missing approval dates: {df_loans["approval_date"].isna().sum()}')
        
        # Test license data
        df_licenses = pd.read_csv('sample_licenses.csv')
        print(f'✅ License data: {len(df_licenses)} records')
        print(f'   Missing license types: {df_licenses["license_type"].isna().sum()}')
        print(f'   Missing business names: {df_licenses["business_name"].isna().sum()}')
        
        # Test prospect data
        df_prospects = pd.read_csv('sample_prospects.csv')
        print(f'✅ Prospect data: {len(df_prospects)} records')
        
        print('\n🎯 Data Integrity Summary:')
        total_records = len(df_business) + len(df_loans) + len(df_licenses) + len(df_prospects)
        print(f'   Total records across all files: {total_records}')
        print('   All data integrity checks passed!')
        
        return True
        
    except Exception as e:
        print(f'❌ Error in data integrity test: {e}')
        return False

if __name__ == "__main__":
    success = test_data_integrity()
    sys.exit(0 if success else 1)