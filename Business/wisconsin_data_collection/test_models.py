#!/usr/bin/env python3
"""
Test script for data models
"""

import sys
sys.path.append('.')
from models import BusinessEntity, SBALoanRecord, BusinessLicense
from datetime import date

def test_models():
    print('🧪 Testing data models with required fields...')

    # Test BusinessEntity with all required fields
    business = BusinessEntity(
        business_id='TEST001',
        source_id='DFI001',
        business_name='Test Restaurant',
        business_type='restaurant',
        city='Milwaukee',
        state='WI',
        registration_date=date.today(),
        status='active',
        data_source='state_registration'
    )
    print(f'✅ Business: {business.business_name} in {business.city}')

    # Test SBA Loan with required fields
    loan = SBALoanRecord(
        loan_id='LOAN001',
        borrower_name='Test Borrower',
        loan_amount=250000.00,
        approval_date=date.today(),
        program_type='7a',
        borrower_city='Madison',
        borrower_state='WI'
    )
    print(f'✅ SBA Loan: {loan.borrower_name} - ${loan.loan_amount:,.2f}')

    # Test License with required fields
    license = BusinessLicense(
        license_id='LIC001',
        business_name='Test License Holder',
        license_type='Restaurant',
        status='Active',
        issue_date=date.today(),
        city='Green Bay',
        state='WI',
        data_source='business_licenses'
    )
    print(f'✅ License: {license.business_name} - {license.license_type}')

    print('✅ All data models working correctly!')

if __name__ == "__main__":
    test_models()