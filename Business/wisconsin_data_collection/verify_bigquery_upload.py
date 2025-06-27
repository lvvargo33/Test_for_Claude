#!/usr/bin/env python3
"""
Verify Google Places BigQuery Upload
"""

from google.cloud import bigquery

def main():
    client = bigquery.Client(project='location-optimizer-1')
    
    # Query the data
    query = '''
    SELECT 
        county_name,
        business_category,
        COUNT(*) as business_count
    FROM `location-optimizer-1.wisconsin_business_data.google_places_raw`
    WHERE county_name IN ('Milwaukee', 'Dane', 'Brown')
    GROUP BY county_name, business_category
    ORDER BY county_name, business_count DESC
    '''
    
    results = client.query(query).result()
    
    print('=== GOOGLE PLACES DATA IN BIGQUERY ===')
    current_county = None
    for row in results:
        if row.county_name != current_county:
            print(f'\n{row.county_name} County:')
            current_county = row.county_name
        print(f'  {row.business_category}: {row.business_count}')
    
    # Get total count
    total_query = 'SELECT COUNT(*) as total FROM `location-optimizer-1.wisconsin_business_data.google_places_raw`'
    total_result = list(client.query(total_query).result())[0]
    print(f'\nTotal businesses in BigQuery: {total_result.total}')

if __name__ == "__main__":
    main()