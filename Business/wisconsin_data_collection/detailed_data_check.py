#!/usr/bin/env python3
"""
Detailed BigQuery Data Coverage Check
"""
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548.json'

def check_detailed_coverage():
    client = bigquery.Client(project="location-optimizer-1")
    
    # Check Google Places data in wisconsin_business_data dataset
    try:
        query = """
        SELECT 
            COUNT(*) as total_businesses,
            COUNT(DISTINCT county_name) as counties_covered,
            MIN(collection_date) as earliest_collection,
            MAX(collection_date) as latest_collection,
            COUNT(DISTINCT search_business_type) as business_types
        FROM `location-optimizer-1.wisconsin_business_data.google_places_raw`
        """
        
        print('🏢 Google Places Data Coverage:')
        for row in client.query(query):
            print(f'   Total businesses: {row.total_businesses:,}')
            print(f'   Counties covered: {row.counties_covered}')
            print(f'   Collection period: {row.earliest_collection} to {row.latest_collection}')
            print(f'   Business types: {row.business_types}')
            
    except Exception as e:
        print(f'   Google Places data error: {e}')

    # Check consumer spending data in more detail
    try:
        query = """
        SELECT 
            state,
            MIN(year) as earliest_year,
            MAX(year) as latest_year,
            COUNT(DISTINCT year) as years_covered,
            COUNT(DISTINCT spending_category) as categories,
            SUM(spending_amount) as total_spending_millions
        FROM `location-optimizer-1.raw_business_data.consumer_spending`
        GROUP BY state
        """
        
        print('\n💰 Consumer Spending Data Coverage:')
        for row in client.query(query):
            print(f'   State: {row.state}')
            print(f'   Years: {row.earliest_year}-{row.latest_year} ({row.years_covered} years)')
            print(f'   Categories: {row.categories}')
            print(f'   Total spending: ${row.total_spending_millions:,.1f} million')
            
    except Exception as e:
        print(f'   Consumer spending data error: {e}')
    
    # Check OSM businesses
    try:
        query = """
        SELECT 
            COUNT(*) as total_businesses,
            COUNT(DISTINCT amenity) as amenity_types,
            COUNT(DISTINCT shop) as shop_types,
            COUNT(DISTINCT name) as named_businesses
        FROM `location-optimizer-1.raw_business_data.osm_businesses`
        """
        
        print('\n🗺️ OpenStreetMap Business Data:')
        for row in client.query(query):
            print(f'   Total businesses: {row.total_businesses:,}')
            print(f'   Named businesses: {row.named_businesses:,}')
            print(f'   Amenity types: {row.amenity_types}')
            print(f'   Shop types: {row.shop_types}')
            
    except Exception as e:
        print(f'   OSM data error: {e}')
    
    # Check traffic data
    try:
        query = """
        SELECT 
            COUNT(*) as total_counts,
            COUNT(DISTINCT county) as counties_covered,
            MIN(measurement_year) as earliest_year,
            MAX(measurement_year) as latest_year,
            AVG(aadt) as avg_daily_traffic
        FROM `location-optimizer-1.raw_traffic.traffic_counts`
        WHERE aadt IS NOT NULL
        """
        
        print('\n🚗 Traffic Count Data:')
        for row in client.query(query):
            print(f'   Total measurements: {row.total_counts:,}')
            print(f'   Counties covered: {row.counties_covered}')
            print(f'   Years: {row.earliest_year}-{row.latest_year}')
            print(f'   Average daily traffic: {row.avg_daily_traffic:,.0f}')
            
    except Exception as e:
        print(f'   Traffic data error: {e}')
    
    # Check BLS employment data
    try:
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT county_name) as counties_covered,
            MIN(year) as earliest_year,
            MAX(year) as latest_year,
            COUNT(DISTINCT measure_type) as measure_types
        FROM `location-optimizer-1.raw_business_data.bls_laus_data`
        """
        
        print('\n📊 BLS Employment Data (LAUS):')
        for row in client.query(query):
            print(f'   Total records: {row.total_records:,}')
            print(f'   Counties covered: {row.counties_covered}')
            print(f'   Years: {row.earliest_year}-{row.latest_year}')
            print(f'   Measure types: {row.measure_types}')
            
    except Exception as e:
        print(f'   BLS employment data error: {e}')

    # Check demographics data
    try:
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT county_fips) as counties_covered,
            COUNT(DISTINCT geographic_level) as geographic_levels
        FROM `location-optimizer-1.raw_business_data.census_demographics`
        """
        
        print('\n👥 Demographics Data:')
        for row in client.query(query):
            print(f'   Total records: {row.total_records:,}')
            print(f'   Counties covered: {row.counties_covered}')
            print(f'   Geographic levels: {row.geographic_levels}')
            
    except Exception as e:
        print(f'   Demographics data error: {e}')

    # Check SBA loan data
    try:
        query = """
        SELECT 
            COUNT(*) as total_loans,
            COUNT(DISTINCT borrower_city) as cities_covered,
            MIN(approval_date) as earliest_date,
            MAX(approval_date) as latest_date,
            SUM(loan_amount) as total_approved_amount,
            COUNT(DISTINCT program_type) as program_types
        FROM `location-optimizer-1.raw_business_data.sba_loan_approvals`
        """
        
        print('\n💰 SBA Loan Data:')
        for row in client.query(query):
            print(f'   Total loans: {row.total_loans:,}')
            print(f'   Cities covered: {row.cities_covered}')
            print(f'   Date range: {row.earliest_date} to {row.latest_date}')
            print(f'   Total approved: ${row.total_approved_amount:,.0f}')
            print(f'   Program types: {row.program_types}')
            
    except Exception as e:
        print(f'   SBA loan data error: {e}')

if __name__ == "__main__":
    check_detailed_coverage()