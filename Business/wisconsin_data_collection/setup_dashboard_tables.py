#!/usr/bin/env python3
"""
Set up BigQuery tables for Financial Institution Dashboard
"""

import json
from google.cloud import bigquery
import os
from datetime import datetime

def setup_financial_dashboard():
    """Set up the financial institution dashboard table in BigQuery"""
    
    # Set up credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548_decrypted.json'
    
    # Initialize BigQuery client
    client = bigquery.Client()
    
    print('📊 Creating Financial Institution Dashboard table...')
    
    # Create the financial institution analysis table
    create_table_sql = '''
    CREATE OR REPLACE TABLE `location-optimizer-1.business_analysis.financial_institution_dashboard` (
      business_name STRING,
      business_type STRING,
      location STRING,
      analysis_date DATE,
      
      -- Credit Analysis Metrics
      credit_risk_score INT64,
      debt_service_coverage_ratio FLOAT64,
      loan_to_value_ratio FLOAT64,
      collateral_adequacy_ratio FLOAT64,
      
      -- SBA Compliance
      sba_eligibility_score INT64,
      recommended_sba_program STRING,
      sba_rationale STRING,
      
      -- Loan Structure
      total_project_cost INT64,
      optimal_loan_amount INT64,
      equity_requirement INT64,
      down_payment_percentage FLOAT64,
      expected_loan_yield FLOAT64,
      
      -- Risk Assessment
      loan_approval_probability FLOAT64,
      regulatory_compliance_score INT64,
      institutional_risk_rating STRING
    )
    '''
    
    job = client.query(create_table_sql)
    job.result()
    print('✅ Table created successfully!')
    
    # Insert sample data
    insert_data_sql = '''
    INSERT INTO `location-optimizer-1.business_analysis.financial_institution_dashboard` VALUES
    ('Sample Indian Restaurant', 'Indian Restaurant', 'Milwaukee, WI', CURRENT_DATE(),
     75, 1.45, 0.75, 1.35, 85, 'SBA 7(a)', 'Lower real estate component favors working capital flexibility',
     325000, 292500, 32500, 0.10, 0.0575, 78.5, 85, 'B'),
     
    ('Sample Auto Repair Shop', 'Auto Repair Shop', 'Green Bay, WI', CURRENT_DATE(),
     68, 1.28, 0.82, 1.22, 92, 'SBA 504', 'High real estate component (62%) favors SBA 504 structure',
     450000, 225000, 45000, 0.10, 0.0525, 82.3, 88, 'B+'),
     
    ('Sample Coffee Shop', 'Coffee Shop', 'Madison, WI', CURRENT_DATE(),
     82, 1.62, 0.68, 1.47, 88, 'SBA 7(a)', 'Service business with moderate real estate needs',
     185000, 166500, 18500, 0.10, 0.0595, 87.2, 92, 'A-'),
     
    ('Sample Hardware Store', 'Hardware Store', 'Kenosha, WI', CURRENT_DATE(),
     71, 1.38, 0.78, 1.28, 79, 'SBA 7(a)', 'Retail business with inventory and equipment focus',
     275000, 247500, 27500, 0.10, 0.0585, 74.8, 81, 'B')
    '''
    
    job = client.query(insert_data_sql)
    job.result()
    print('✅ Sample data inserted successfully!')
    
    # Verify the data
    verify_sql = '''
    SELECT 
      business_name,
      credit_risk_score,
      debt_service_coverage_ratio,
      sba_eligibility_score,
      recommended_sba_program,
      optimal_loan_amount,
      institutional_risk_rating
    FROM `location-optimizer-1.business_analysis.financial_institution_dashboard`
    ORDER BY credit_risk_score DESC
    '''
    
    job = client.query(verify_sql)
    results = job.result()
    
    print('\n📊 Financial Institution Dashboard Data:')
    print('=' * 80)
    for row in results:
        print(f'Business: {row.business_name}')
        print(f'Credit Score: {row.credit_risk_score}/100 | DSCR: {row.debt_service_coverage_ratio:.2f}')
        print(f'SBA Score: {row.sba_eligibility_score}/100 | Program: {row.recommended_sba_program}')
        print(f'Loan Amount: ${row.optimal_loan_amount:,} | Risk Rating: {row.institutional_risk_rating}')
        print('-' * 80)
    
    print('\n🎯 Next Step: Connect Looker Studio to this BigQuery table!')
    print('Table: location-optimizer-1.business_analysis.financial_institution_dashboard')
    
    return True

if __name__ == '__main__':
    setup_financial_dashboard()