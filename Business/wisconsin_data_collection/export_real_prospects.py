#!/usr/bin/env python3
"""
Export real Wisconsin SBA prospects
"""

from google.cloud import bigquery
import pandas as pd
import os

def export_real_prospects():
    client = bigquery.Client()

    # Export high-value prospects from real SBA data
    query = '''
    SELECT 
        'SBA_LOAN' as source,
        borrower_name as business_name,
        borrower_city as city,
        borrower_state as state,
        borrower_zip as zip_code,
        CONCAT('Phone: Congratulations on your $', FORMAT('%d', CAST(loan_amount as INT64)), ' SBA loan! Free location expansion analysis available') as contact_approach,
        loan_amount as value_indicator,
        CAST(approval_date AS STRING) as key_date,
        DATE_DIFF(CURRENT_DATE(), approval_date, DAY) as days_since_approval,
        1 as priority_score,
        CASE 
            WHEN loan_amount >= 2000000 THEN 'HIGH'
            WHEN loan_amount >= 1000000 THEN 'MEDIUM'
            WHEN loan_amount >= 500000 THEN 'QUALIFIED'
            ELSE 'STANDARD'
        END as lead_quality,
        business_type,
        naics_code,
        franchise_name
    FROM `location-optimizer-1.raw_business_data.sba_loan_approvals`
    WHERE borrower_state = 'WI'
        AND approval_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)  -- Last 2 years
        AND loan_amount >= 500000  -- Focus on larger loans
    ORDER BY loan_amount DESC, approval_date DESC
    LIMIT 50
    '''

    print('🎯 Exporting Real Wisconsin SBA Prospects...')
    df = client.query(query).to_dataframe()

    # Save to CSV
    filename = 'real_wisconsin_sba_prospects.csv'
    df.to_csv(filename, index=False)

    print(f'✅ Exported {len(df)} high-value Wisconsin SBA prospects to {filename}')
    print(f'\n📋 Top 10 Prospects:')

    for i, row in df.head(10).iterrows():
        print(f'   {i+1}. {row["business_name"]} ({row["city"]})')
        print(f'      💰 ${row["value_indicator"]:,.0f} - {row["lead_quality"]} quality')
        print(f'      📞 Approach: {row["contact_approach"][:80]}...')
        if pd.notna(row['franchise_name']) and row['franchise_name']:
            print(f'      🔗 Franchise: {row["franchise_name"]}')
        print()

    # Summary stats
    print(f'📊 Prospect Summary:')
    print(f'   Total prospects: {len(df)}')
    print(f'   Average loan size: ${df["value_indicator"].mean():,.0f}')
    print(f'   Total funding represented: ${df["value_indicator"].sum():,.0f}')
    print(f'   Lead quality distribution:')
    for quality, count in df['lead_quality'].value_counts().items():
        print(f'     {quality}: {count} prospects')

    return len(df)

if __name__ == "__main__":
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './location-optimizer-1-449414f93a5a.json'
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'location-optimizer-1'
    
    result = export_real_prospects()
    print(f"\n🎉 Successfully exported {result} real prospects!")