#!/usr/bin/env python3
"""
Verify OSM Data in BigQuery
===========================

Check that OSM data was successfully stored in BigQuery tables.
"""

import os
from google.cloud import bigquery

def verify_osm_data():
    """Verify OSM data was successfully stored in BigQuery"""
    
    # Set up credentials
    credentials_path = "/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    client = bigquery.Client(project="location-optimizer-1")
    dataset_id = "raw_business_data"
    
    print("🔍 Verifying OSM Data in BigQuery")
    print("=" * 40)
    
    # Check osm_businesses table
    print("\n📊 OSM Businesses Table:")
    businesses_query = f"""
    SELECT 
        COUNT(*) as total_businesses,
        COUNT(DISTINCT address_city) as unique_cities,
        COUNT(CASE WHEN franchise_indicator = true THEN 1 END) as franchise_count,
        business_type,
        COUNT(*) as count_by_type
    FROM `location-optimizer-1.{dataset_id}.osm_businesses`
    GROUP BY business_type
    ORDER BY count_by_type DESC
    """
    
    try:
        results = client.query(businesses_query).result()
        
        total_businesses = 0
        business_types = []
        
        for row in results:
            if total_businesses == 0:  # First row gives us totals
                total_businesses = row.total_businesses
                unique_cities = row.unique_cities  
                franchise_count = row.franchise_count
            
            business_types.append({
                'type': row.business_type,
                'count': row.count_by_type
            })
        
        print(f"   Total Businesses: {total_businesses}")
        print(f"   Unique Cities: {unique_cities}")
        print(f"   Franchise Businesses: {franchise_count}")
        
        print(f"\n📈 Business Type Breakdown:")
        for btype in business_types:
            percentage = (btype['count'] / total_businesses) * 100 if total_businesses > 0 else 0
            print(f"   {btype['type']}: {btype['count']} ({percentage:.1f}%)")
            
    except Exception as e:
        print(f"   ❌ Error querying businesses table: {e}")
    
    # Check osm_collection_summary table
    print(f"\n📋 OSM Collection Summary Table:")
    summary_query = f"""
    SELECT 
        collection_date,
        area_name,
        businesses_collected,
        franchises_identified,
        cities_covered,
        avg_data_quality_score,
        businesses_with_contact,
        businesses_with_address,
        processing_time_seconds
    FROM `location-optimizer-1.{dataset_id}.osm_collection_summary`
    ORDER BY collection_date DESC
    LIMIT 5
    """
    
    try:
        results = client.query(summary_query).result()
        
        for row in results:
            print(f"   Collection: {row.collection_date}")
            print(f"   Area: {row.area_name}")
            print(f"   Businesses: {row.businesses_collected}")
            print(f"   Franchises: {row.franchises_identified}")
            print(f"   Cities: {row.cities_covered}")
            print(f"   Quality Score: {row.avg_data_quality_score:.1f}/100")
            print(f"   With Contact: {row.businesses_with_contact}")
            print(f"   With Address: {row.businesses_with_address}")
            print(f"   Processing Time: {row.processing_time_seconds:.1f}s")
            print()
            
    except Exception as e:
        print(f"   ❌ Error querying summary table: {e}")
    
    # Sample businesses
    print(f"\n🏪 Sample Businesses:")
    sample_query = f"""
    SELECT 
        name,
        business_type,
        address_city,
        address_street,
        brand,
        franchise_indicator,
        phone,
        website
    FROM `location-optimizer-1.{dataset_id}.osm_businesses`
    WHERE name IS NOT NULL
    ORDER BY data_collection_date DESC
    LIMIT 10
    """
    
    try:
        results = client.query(sample_query).result()
        
        for i, row in enumerate(results, 1):
            print(f"   {i}. {row.name}")
            print(f"      Type: {row.business_type}")
            print(f"      Location: {row.address_street}, {row.address_city}")
            if row.brand:
                print(f"      Brand: {row.brand}")
            if row.franchise_indicator:
                print(f"      🏢 Franchise")
            if row.phone:
                print(f"      📞 {row.phone}")
            if row.website:
                print(f"      🌐 {row.website}")
            print()
            
    except Exception as e:
        print(f"   ❌ Error querying sample businesses: {e}")

if __name__ == "__main__":
    verify_osm_data()