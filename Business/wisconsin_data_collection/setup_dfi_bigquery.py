#!/usr/bin/env python3
"""
Setup BigQuery schema for DFI business data
"""

from google.cloud import bigquery
import os

def setup_dfi_bigquery():
    client = bigquery.Client()

    # Create new table for DFI business registrations
    table_id = 'location-optimizer-1.raw_business_data.dfi_business_registrations'

    # Check if table exists and delete it
    try:
        client.delete_table(table_id)
        print('✅ Deleted existing DFI table')
    except:
        print('ℹ️  DFI table did not exist')

    # Create schema for DFI business data
    schema = [
        bigquery.SchemaField('business_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('business_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('entity_type', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('registration_date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('status', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('business_type', 'STRING'),
        bigquery.SchemaField('agent_name', 'STRING'),
        bigquery.SchemaField('business_address', 'STRING'),
        bigquery.SchemaField('city', 'STRING'),
        bigquery.SchemaField('state', 'STRING'),
        bigquery.SchemaField('zip_code', 'STRING'),
        bigquery.SchemaField('county', 'STRING'),
        bigquery.SchemaField('naics_code', 'STRING'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('data_extraction_date', 'TIMESTAMP'),
        bigquery.SchemaField('is_target_business', 'BOOLEAN'),
    ]

    table = bigquery.Table(table_id, schema=schema)

    # Add partitioning by extraction date
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='data_extraction_date'
    )

    # Add clustering for efficient queries
    table.clustering_fields = ['business_type', 'county', 'registration_date']

    table = client.create_table(table)
    print(f'✅ Created DFI business registrations table: {table_id}')

    # Create a unified business opportunities view
    view_query = """
    SELECT 
        'DFI' as data_source,
        business_id,
        business_name,
        business_type,
        city,
        state,
        zip_code,
        county,
        registration_date as key_date,
        CAST(NULL as FLOAT64) as value_indicator,
        'New Business Registration' as opportunity_type,
        data_extraction_date
    FROM `location-optimizer-1.raw_business_data.dfi_business_registrations`
    WHERE is_target_business = true

    UNION ALL

    SELECT 
        'SBA' as data_source,
        loan_id as business_id,
        borrower_name as business_name,
        CASE 
            WHEN business_type = 'CORPORATION' THEN 'business'
            ELSE LOWER(business_type)
        END as business_type,
        borrower_city as city,
        borrower_state as state,
        borrower_zip as zip_code,
        CAST(NULL as STRING) as county,
        approval_date as key_date,
        loan_amount as value_indicator,
        'SBA Loan Approval' as opportunity_type,
        data_extraction_date
    FROM `location-optimizer-1.raw_business_data.sba_loan_approvals`

    UNION ALL

    SELECT 
        'LICENSE' as data_source,
        license_id as business_id,
        business_name,
        'license' as business_type,
        city,
        state,
        zip_code,
        CAST(NULL as STRING) as county,
        issue_date as key_date,
        CAST(NULL as FLOAT64) as value_indicator,
        'Business License' as opportunity_type,
        data_extraction_date
    FROM `location-optimizer-1.raw_business_data.business_licenses`
    WHERE state = 'WI'
    """

    view_id = 'location-optimizer-1.business_analytics.unified_business_opportunities'
    view = bigquery.Table(view_id)
    view.view_query = view_query

    try:
        client.delete_table(view_id)
        print('✅ Deleted existing unified view')
    except:
        pass

    view = client.create_table(view)
    print(f'✅ Created unified business opportunities view: {view_id}')

if __name__ == "__main__":
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './location-optimizer-1-449414f93a5a.json'
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'location-optimizer-1'
    
    setup_dfi_bigquery()