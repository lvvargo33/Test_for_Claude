#!/usr/bin/env python3
"""
Verify Combined Phase 1 + Phase 2 + Phase 3 Google Places Data in BigQuery
"""

from google.cloud import bigquery

def main():
    client = bigquery.Client(project='location-optimizer-1')
    
    # Query the combined data for all phases
    query = '''
    SELECT 
        county_name,
        business_category,
        COUNT(*) as business_count
    FROM `location-optimizer-1.wisconsin_business_data.google_places_raw`
    WHERE county_name IN ('Milwaukee', 'Dane', 'Brown', 'Winnebago', 'Eau Claire', 'Marathon', 'Kenosha', 'Racine', 'Sauk', 'Door', 'La Crosse', 'Portage', 'Douglas', 'Rock')
    GROUP BY county_name, business_category
    ORDER BY county_name, business_count DESC
    '''
    
    results = client.query(query).result()
    
    print('=== COMPLETE GOOGLE PLACES DATA (ALL PHASES) ===')
    current_county = None
    county_totals = {}
    
    for row in results:
        if row.county_name != current_county:
            if current_county and current_county in county_totals:
                print(f'  Total: {county_totals[current_county]}')
            print(f'\n{row.county_name} County:')
            current_county = row.county_name
            county_totals[current_county] = county_totals.get(current_county, 0) + row.business_count
        else:
            county_totals[current_county] += row.business_count
        
        if row.business_count >= 5:  # Only show categories with 5+ businesses
            print(f'  {row.business_category}: {row.business_count}')
    
    if current_county and current_county in county_totals:
        print(f'  Total: {county_totals[current_county]}')
    
    # Get overall summary
    summary_query = '''
    SELECT 
        COUNT(*) as total_businesses,
        COUNT(DISTINCT county_name) as counties_covered,
        COUNT(DISTINCT business_category) as business_categories
    FROM `location-optimizer-1.wisconsin_business_data.google_places_raw`
    '''
    
    summary = list(client.query(summary_query).result())[0]
    
    print(f'\n=== OVERALL SUMMARY ===')
    print(f'Total Businesses: {summary.total_businesses:,}')
    print(f'Counties Covered: {summary.counties_covered}')
    print(f'Business Categories: {summary.business_categories}')
    
    # Phase breakdown
    phase_query = '''
    SELECT 
        CASE 
            WHEN county_name IN ('Milwaukee', 'Dane', 'Brown') THEN 'Phase 1'
            WHEN county_name IN ('Winnebago', 'Eau Claire', 'Marathon', 'Kenosha', 'Racine') THEN 'Phase 2'
            WHEN county_name IN ('Sauk', 'Door', 'La Crosse', 'Portage', 'Douglas', 'Rock') THEN 'Phase 3'
            ELSE 'Other'
        END as phase,
        COUNT(*) as businesses
    FROM `location-optimizer-1.wisconsin_business_data.google_places_raw`
    GROUP BY phase
    ORDER BY phase
    '''
    
    phase_results = client.query(phase_query).result()
    print(f'\n=== PHASE BREAKDOWN ===')
    for row in phase_results:
        print(f'{row.phase}: {row.businesses:,} businesses')
    
    # Top counties
    county_query = '''
    SELECT 
        county_name,
        COUNT(*) as businesses
    FROM `location-optimizer-1.wisconsin_business_data.google_places_raw`
    GROUP BY county_name
    ORDER BY businesses DESC
    '''
    
    county_results = client.query(county_query).result()
    print(f'\n=== TOP COUNTIES BY BUSINESS COUNT ===')
    for i, row in enumerate(county_results, 1):
        print(f'{i:2d}. {row.county_name}: {row.businesses:,} businesses')
    
    # Top business categories overall
    category_query = '''
    SELECT 
        business_category,
        COUNT(*) as businesses
    FROM `location-optimizer-1.wisconsin_business_data.google_places_raw`
    GROUP BY business_category
    ORDER BY businesses DESC
    LIMIT 10
    '''
    
    category_results = client.query(category_query).result()
    print(f'\n=== TOP BUSINESS CATEGORIES ===')
    for i, row in enumerate(category_results, 1):
        print(f'{i:2d}. {row.business_category}: {row.businesses:,} businesses')

if __name__ == "__main__":
    main()